% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_cluster_view).

-export([query_view/5]).

-include("couch_db.hrl").
-include("couch_api_wrap.hrl").

-define(MAX_QUEUE_ITEMS, 3).
-define(NYI, {not_yet_implemented, ?MODULE, ?LINE}).

-import(couch_util, [
    get_value/2,
    get_value/3,
    get_nested_json_value/2
]).


query_view(DDocId, ViewName, DbNames, Keys, #httpd{user_ctx = UserCtx} = Req) ->
    ViewDef = check_view_exists(DbNames, UserCtx, DDocId, ViewName, nil),
    % TODO: support reduce views
    map = ViewType = view_type(ViewDef, Req),
    ViewArgs = #view_query_args{
        skip = Skip,
        limit = Limit,
        direction = Dir
    } = couch_httpd_view:parse_view_params(Req, Keys, ViewType),
    LessFun = view_less_fun(ViewDef, Dir),
    {Queues, Folders} = lists:foldr(
        fun(DbName, {QAcc, PidAcc}) ->
            {ok, Q} = couch_work_queue:new([{max_items, ?MAX_QUEUE_ITEMS}]),
            Pid = spawn_link(fun() ->
                map_view_folder(DbName, UserCtx, DDocId, ViewName, Keys, ViewArgs, Q)
            end),
            {[Q | QAcc], [Pid | PidAcc]}
        end,
        {[], []}, DbNames),
    Sender = spawn_link(fun() -> http_sender_loop(Req, length(Queues)) end),
    case merge_map_views(Queues, dict:new(), LessFun, Sender, Skip, Limit) of
    {ok, Resp} ->
        Resp;
    {stop, Resp} ->
        lists:foreach(
            fun(P) -> catch unlink(P), catch exit(P, kill) end, Folders),
        lists:foreach(
            fun(P) -> catch unlink(P), catch exit(P, kill) end, Queues),
        Resp
    end.


view_less_fun({ViewDef}, Dir) ->
    {ViewOptions} = get_value(<<"options">>, ViewDef, {[]}),
    LessFun = case get_value(<<"collation">>, ViewOptions, <<"default">>) of
    <<"default">> ->
        fun(RowA, RowB) ->
            couch_view:less_json_ids(element(1, RowA), element(1, RowB))
        end;
    <<"raw">> ->
        fun(A, B) -> A < B end
    end,
    case Dir of
    fwd ->
        LessFun;
    rev ->
        fun(A, B) -> not LessFun(A, B) end
    end.


view_type({ViewDef}, Req) ->
    true = is_binary(get_value(<<"map">>, ViewDef)),
    case get_value(<<"reduce">>, ViewDef) of
    undefined ->
        map;
    RedFun when is_binary(RedFun) ->
        case couch_httpd:qs_value(Req, "reduce", "true") of
        "true" ->
            reduce;
        "false" ->
            map
        end
    end.


http_sender_loop(Req, NumFolders) ->
    http_sender_collect_row_count(Req, NumFolders, 0).

http_sender_collect_row_count(Req, RecvCount, AccCount) ->
    receive
    {row_count, Count} ->
        AccCount2 = AccCount + Count,
        case RecvCount > 1 of
        false ->
            % TODO: what about offset and update_seq?
            % TODO: maybe add etag like for regular views? How to
            %       compute them?
            Start = io_lib:format(
                "{\"total_rows\":~w,\"rows\":[\r\n", [AccCount2]),
            {ok, Resp} = couch_httpd:start_json_response(Req, 200, []),
            couch_httpd:send_chunk(Resp, Start),
            http_sender_send_rows(Resp, <<"\r\n">>);
        true ->
            http_sender_collect_row_count(Req, RecvCount - 1, AccCount2)
        end
    end.


http_sender_send_rows(Resp, Acc) ->
    receive
    {row, Row} ->
        RowEJson = view_row_obj(Row),
        couch_httpd:send_chunk(Resp, [Acc, ?JSON_ENCODE(RowEJson)]),
        http_sender_send_rows(Resp, <<",\r\n">>);
    {stop, From} ->
        couch_httpd:send_chunk(Resp, <<"\r\n]}">>),
        Res = couch_httpd:end_json_response(Resp),
        From ! {self(), Res}
    end.


view_row_obj({{Key, error}, Value}) ->
    {[{key, Key}, {error, Value}]};

view_row_obj({{Key, DocId}, Value}) ->
    {[{id, DocId}, {key, Key}, {value, Value}]};

view_row_obj({{Key, DocId}, Value, Doc}) ->
    {[{id, DocId}, {key, Key}, {value, Value}, Doc]}.


% NOTE: this merge logic will be different for reduce views
merge_map_views([], _QueueMap, _LessFun, Sender, _Skip, _Limit) ->
    Sender ! {stop, self()},
    receive
    {Sender, Resp} ->
        {ok, Resp}
    end;

merge_map_views(_Queues, _QueueMap, _LessFun, Sender, _Skip, 0) ->
    Sender ! {stop, self()},
    receive
    {Sender, Resp} ->
        {stop, Resp}
    end;

merge_map_views(Queues, QueueMap, LessFun, Sender, Skip, Limit) ->
    % QueueMap, map the last row taken from each queue to its respective
    % queue. Each row in this dict/map is a row that was not the smallest
    % one in the previous iteration.
    case dequeue(Queues, QueueMap, Sender) of
    {[], _, Queues2} ->
        merge_map_views(Queues2, QueueMap, LessFun, Sender, Skip, Limit);
    {TopRows, RowsToQueuesMap, Queues2} ->
        {SmallestRow, RestRows} = take_smallest_row(TopRows, LessFun),
        [QueueSmallest | _] = dict:fetch(SmallestRow, RowsToQueuesMap),
        QueueMap2 = lists:foldl(
            fun(R, Acc) ->
                QList = dict:fetch(R, RowsToQueuesMap),
                lists:foldl(fun(Q, D) -> dict:store(Q, R, D) end, Acc, QList)
            end,
            dict:erase(QueueSmallest, QueueMap),
            RestRows),
        case Skip > 0 of
        true ->
            Limit2 = Limit;
        false ->
            Sender ! {row, SmallestRow},
            Limit2 = dec_counter(Limit)
        end,
        merge_map_views(
            Queues2, QueueMap2, LessFun, Sender, dec_counter(Skip), Limit2)
    end.


dec_counter(0) -> 0;
dec_counter(N) -> N - 1.


dequeue(Queues, QueueMap, Sender) ->
    % need to keep track from which queues each row was taken
    RowsToQueuesMap0 = dict:new(),
    % order of TopRows is important
    {TopRows, RowsToQueuesMap1, ClosedQueues} = lists:foldr(
        fun(Q, {RowAcc, RMap, Closed}) ->
            case dict:find(Q, QueueMap) of
            {ok, Row} ->
                {[Row | RowAcc], dict:append(Row, Q, RMap), Closed};
            error ->
                case couch_work_queue:dequeue(Q, 1) of
                {ok, [{row_count, _} = RowCount]} ->
                    Sender ! RowCount,
                    case couch_work_queue:dequeue(Q, 1) of
                    {ok, [Row]} ->
                        {[Row | RowAcc], dict:append(Row, Q, RMap), Closed};
                    closed ->
                        {RowAcc, RMap, [Q | Closed]}
                    end;
                {ok, [Row]} ->
                    {[Row | RowAcc], dict:append(Row, Q, RMap), Closed};
                closed ->
                    {RowAcc, RMap, [Q | Closed]}
                end
            end
        end,
        {[], RowsToQueuesMap0, []}, Queues),
   {TopRows, RowsToQueuesMap1, Queues -- ClosedQueues}.


take_smallest_row([First | Rest], LessFun) ->
    take_smallest_row(Rest, First, LessFun, []).

take_smallest_row([], Smallest, _LessFun, Acc) ->
    {Smallest, Acc};
take_smallest_row([Row | Rest], Smallest, LessFun, Acc) ->
    case LessFun(Row, Smallest) of
    true ->
        take_smallest_row(Rest, Row, LessFun, [Smallest | Acc]);
    false ->
        take_smallest_row(Rest, Smallest, LessFun, [Row | Acc])
    end.


map_view_folder(<<"http://", _/binary>> = _DbName, _UserCtx,
                _DDocId, _ViewName, _Keys, _ViewArgs, _Queue) ->
    % TODO
    throw(?NYI);
map_view_folder(<<"https://", _/binary>> = _DbName, _UserCtx,
                _DDocId, _ViewName, _Keys, _ViewArgs, _Queue) ->
    % TODO
    throw(?NYI);
map_view_folder(DbName, UserCtx, DDocId, ViewName, Keys, ViewArgs, Queue) ->
    #view_query_args{
        stale = Stale,
        include_docs = IncludeDocs,
        conflicts = Conflicts
    } = ViewArgs,
    {ok, Db} = couch_db:open(DbName, [{user_ctx, UserCtx}]),
    try
        FoldlFun = make_map_fold_fun(IncludeDocs, Conflicts, Db, Queue),
        View = get_map_view(Db, DDocId, ViewName, Stale),
        {ok, RowCount} = couch_view:get_row_count(View),
        couch_work_queue:queue(Queue, {row_count, RowCount}),
        case Keys of
        nil ->
            FoldOpts = couch_httpd_view:make_key_options(ViewArgs),
            {ok, _, _} = couch_view:fold(View, FoldlFun, [], FoldOpts);
        _ when is_list(Keys) ->
            lists:foreach(
                fun(K) ->
                    FoldOpts = couch_httpd_view:make_key_options(
                        ViewArgs#view_query_args{start_key = K, end_key = K}),
                    {ok, _, _} = couch_view:fold(View, FoldlFun, [], FoldOpts)
                end,
                Keys)
        end,
        couch_work_queue:close(Queue)
    after
        couch_db:close(Db)
    end.


get_map_view(Db, DDocId, ViewName, Stale) ->
    case couch_view:get_map_view(Db, DDocId, ViewName, Stale) of
    {ok, MapView, _} ->
        MapView;
    {not_found, _} ->
        {ok, View, _} = couch_view:get_reduce_view(Db, DDocId, ViewName, Stale),
        couch_view:extract_map_view(View)
    end.


make_map_fold_fun(false, _Conflicts, _Db, Queue) ->
    fun(Row, _, Acc) ->
        couch_work_queue:queue(Queue, Row),
        {ok, Acc}
    end;

make_map_fold_fun(true, Conflicts, Db, Queue) ->
    DocOpenOpts = if Conflicts -> [conflicts]; true -> [] end,
    fun({{_Key, error}, _Value} = Row, _, Acc) ->
        couch_work_queue:queue(Queue, Row),
        {ok, Acc};
    ({{_Key, DocId} = Kd, {Props} = Value}, _, Acc) ->
        Rev = case get_value(<<"_rev">>, Props, nil) of
        nil ->
            nil;
        Rev0 ->
            couch_doc:parse_rev(Rev0)
        end,
        IncludeId = get_value(<<"_id">>, Props, DocId),
        [Doc] = couch_httpd_view:doc_member(Db, {IncludeId, Rev}, DocOpenOpts),
        couch_work_queue:queue(Queue, {Kd, Value, Doc}),
        {ok, Acc};
    ({{_Key, DocId} = Kd, Value}, _, Acc) ->
        [Doc] = couch_httpd_view:doc_member(Db, {DocId, nil}, DocOpenOpts),
        couch_work_queue:queue(Queue, {Kd, Value, Doc}),
        {ok, Acc}
    end.


check_view_exists([], _UserCtx, _DDocId, _ViewName, ViewDef) ->
    ViewDef;
check_view_exists([DbName | Rest], UserCtx, DDocId, ViewName, ViewDef) ->
    {ok, Db} = open_db(DbName, UserCtx),
    #doc{body = Body} = get_ddoc(Db, DDocId),
    couch_api_wrap:db_close(Db),
    ThisViewDef = try
        get_nested_json_value(Body, [<<"views">>, ViewName])
    catch throw:_ ->
        throw({<<"missing_view_in_db">>, ?l2b(couch_api_wrap:db_uri(Db))})
    end,
    case ViewDef of
    nil ->
        check_view_exists(Rest, UserCtx, DDocId, ViewName, ThisViewDef);
    ThisViewDef ->
        check_view_exists(Rest, UserCtx, DDocId, ViewName, ViewDef);
    _ ->
        throw({<<"view_defs_dont_match">>, DDocId, ViewName})
    end.


open_db(<<"http://", _/binary>> = _DbName, _UserCtx) ->
    % TODO: return an #httpdb{} record
    throw(?NYI);
open_db(<<"https://", _/binary>> = _DbName, _UserCtx) ->
    % TODO: return an #httpdb{} record
    throw(?NYI);
open_db(DbName, UserCtx) ->
    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
    {ok, _} = Ok ->
        Ok;
    Error ->
        throw({<<"db_open_error">>, DbName, Error})
    end.


get_ddoc(Db, Id) ->
    case couch_api_wrap:open_doc(Db, Id, [ejson_body]) of
    {ok, Doc} ->
        Doc;
    {error, Error} ->
        throw({<<"ddoc_open_error">>, ?l2b(couch_api_wrap:db_uri(Db)), Error})
    end.
