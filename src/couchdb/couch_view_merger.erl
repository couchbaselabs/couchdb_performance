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

-module(couch_view_merger).

-export([query_view/2]).

-include("couch_db.hrl").
-include("couch_view_merger.hrl").

-define(MAX_QUEUE_ITEMS, 3).
-define(NYI, {not_yet_implemented, ?MODULE, ?LINE}).

-import(couch_util, [
    get_value/2,
    get_value/3,
    get_nested_json_value/2
]).

-record(merge_params, {
    queues,
    queue_map = dict:new(),
    view_def,
    view_lang,
    less_fun,
    collector,
    skip,
    limit
}).

-record(httpdb, {
   url,
   timeout,
   headers = [{"Accept", "application/json"}],
   ibrowse_options = []
}).


query_view(#httpd{user_ctx = UserCtx} = Req, ViewMergeParams) ->
    #view_merge{
       ddoc_id = DDocId, view_name = ViewName, databases = DbNames,
       keys = Keys, callback = Callback, user_acc = UserAcc
    } = ViewMergeParams,
    {Props} = DDoc = check_view_exists(DbNames, UserCtx, ViewMergeParams, nil),
    ViewDef = get_nested_json_value(DDoc, [<<"views">>, ViewName]),
    ViewLang = get_value(<<"language">>, Props, <<"javascript">>),
    ViewType = view_type(ViewDef, Req),
    ViewArgs = couch_httpd_view:parse_view_params(Req, Keys, ViewType),
    LessFun = view_less_fun(ViewDef, ViewArgs#view_query_args.direction, ViewType),
    {FoldFun, MergeFun} = case ViewType of
    reduce ->
        {fun red_view_folder/8, fun merge_red_views/1};
    _ when ViewType =:= map; ViewType =:= red_map ->
        {fun map_view_folder/8, fun merge_map_views/1}
    end,
    {Queues, Folders} = lists:foldr(
        fun(DbName, {QAcc, PidAcc}) ->
            {ok, Q} = couch_work_queue:new([{max_items, ?MAX_QUEUE_ITEMS}]),
            Pid = spawn_link(fun() ->
                FoldFun(
                    DbName, ViewMergeParams, UserCtx,
                    DDocId, ViewName, Keys, ViewArgs, Q)
            end),
            {[Q | QAcc], [Pid | PidAcc]}
        end,
        {[], []}, DbNames),
    Collector = spawn_link(fun() ->
        collector_loop(ViewType, length(Queues), Callback, UserAcc)
    end),
    MergeParams = #merge_params{
        queues = Queues,
        view_def = ViewDef,
        view_lang = ViewLang,
        less_fun = LessFun,
        collector = Collector,
        skip = ViewArgs#view_query_args.skip,
        limit = ViewArgs#view_query_args.limit
    },
    case MergeFun(MergeParams) of
    {ok, Resp} ->
        Resp;
    {stop, Resp} ->
        lists:foreach(
            fun(P) -> catch unlink(P), catch exit(P, kill) end, Folders),
        lists:foreach(
            fun(P) -> catch unlink(P), catch exit(P, kill) end, Queues),
        Resp
    end.


view_less_fun({ViewDef}, Dir, ViewType) ->
    {ViewOptions} = get_value(<<"options">>, ViewDef, {[]}),
    LessFun = case get_value(<<"collation">>, ViewOptions, <<"default">>) of
    <<"default">> ->
        case ViewType of
        _ when ViewType =:= map; ViewType =:= red_map ->
            fun(RowA, RowB) ->
                couch_view:less_json_ids(element(1, RowA), element(1, RowB))
            end;
        reduce ->
            fun({KeyA, _}, {KeyB, _}) -> couch_view:less_json(KeyA, KeyB) end
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
            red_map
        end
    end.


collector_loop(red_map, NumFolders, Callback, UserAcc) ->
    collector_loop(map, NumFolders, Callback, UserAcc);

collector_loop(map, NumFolders, Callback, UserAcc) ->
    collect_row_count(map, NumFolders, 0, Callback, UserAcc);

collector_loop(reduce, _NumFolders, Callback, UserAcc) ->
    {ok, UserAcc2} = Callback(start, UserAcc),
    collect_rows(reduce, Callback, UserAcc2).


collect_row_count(ViewType, RecvCount, AccCount, Callback, UserAcc) ->
    receive
    {{error, _DbUrl, _Reason} = Error, From} ->
        case Callback(Error, UserAcc) of
        {stop, Resp} ->
            From ! {stop, Resp, self()};
        {ok, UserAcc2} ->
            From ! {continue, self()},
            case RecvCount > 1 of
            false ->
                {ok, UserAcc3} = Callback({start, AccCount}, UserAcc2),
                collect_rows(ViewType, Callback, UserAcc3);
            true ->
                collect_row_count(
                    ViewType, RecvCount - 1, AccCount, Callback, UserAcc2)
            end
        end;
    {row_count, Count} ->
        AccCount2 = AccCount + Count,
        case RecvCount > 1 of
        false ->
            % TODO: what about offset and update_seq?
            % TODO: maybe add etag like for regular views? How to
            %       compute them?
            {ok, UserAcc2} = Callback({start, AccCount2}, UserAcc),
            collect_rows(ViewType, Callback, UserAcc2);
        true ->
            collect_row_count(
                ViewType, RecvCount - 1, AccCount2, Callback, UserAcc)
        end
    end.


collect_rows(ViewType, Callback, UserAcc) ->
    receive
    {{error, _DbUrl, _Reason} = Error, From} ->
        case Callback(Error, UserAcc) of
        {stop, Resp} ->
            From ! {stop, Resp, self()};
        {ok, UserAcc2} ->
            From ! {continue, self()},
            collect_rows(ViewType, Callback, UserAcc2)
        end;
    {row, Row} ->
        RowEJson = view_row_obj(ViewType, Row),
        {ok, UserAcc2} = Callback({row, RowEJson}, UserAcc),
        collect_rows(ViewType, Callback, UserAcc2);
    {stop, From} ->
        {ok, UserAcc2} = Callback(stop, UserAcc),
        From ! {UserAcc2, self()}
    end.


view_row_obj(map, {{Key, error}, Value}) ->
    {[{key, Key}, {error, Value}]};

view_row_obj(map, {{Key, DocId}, Value}) ->
    {[{id, DocId}, {key, Key}, {value, Value}]};

view_row_obj(map, {{Key, DocId}, Value, Doc}) ->
    {[{id, DocId}, {key, Key}, {value, Value}, Doc]};

view_row_obj(reduce, {Key, Value}) ->
    {[{key, Key}, {value, Value}]}.


merge_map_views(#merge_params{queues = [], collector = Col}) ->
    Col ! {stop, self()},
    receive
    {Resp, Col} ->
        {ok, Resp}
    end;

merge_map_views(#merge_params{limit = 0, collector = Col}) ->
    Col ! {stop, self()},
    receive
    {Resp, Col} ->
        {stop, Resp}
    end;

merge_map_views(Params) ->
    #merge_params{
        queues = Queues, less_fun = LessFun, queue_map = QueueMap,
        limit = Limit, skip = Skip, collector = Col
    } = Params,
    % QueueMap, map the last row taken from each queue to its respective
    % queue. Each row in this dict/map is a row that was not the smallest
    % one in the previous iteration.
    case (catch dequeue(Queues, QueueMap, Col)) of
    {stop, _Resp} = Stop ->
        Stop;
    {[], _, Queues2} ->
        merge_map_views(Params#merge_params{queues = Queues2});
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
            Col ! {row, SmallestRow},
            Limit2 = dec_counter(Limit)
        end,
        Params2 = Params#merge_params{
            queues = Queues2, queue_map = QueueMap2,
            skip = dec_counter(Skip), limit = Limit2
        },
        merge_map_views(Params2)
    end.


merge_red_views(#merge_params{queues = [], collector = Col}) ->
    Col ! {stop, self()},
    receive
    {Resp, Col} ->
        {ok, Resp}
    end;

merge_red_views(#merge_params{limit = 0, collector = Col}) ->
    Col ! {stop, self()},
    receive
    {Resp, Col} ->
        {stop, Resp}
    end;

merge_red_views(Params) ->
    #merge_params{
        queues = Queues, less_fun = LessFun, queue_map = QueueMap,
        limit = Limit, skip = Skip, collector = Col
    } = Params,
    % QueueMap, map the last row taken from each queue to its respective
    % queue. Each row in this dict/map is a row that was not the smallest
    % one in the previous iteration.
    case (catch dequeue(Queues, QueueMap, Col)) of
    {stop, _Resp} = Stop ->
        Stop;
    {[], _, Queues2} ->
        merge_red_views(Params#merge_params{queues = Queues2});
    {TopRows, RowsToQueuesMap, Queues2} ->
        SortedRows = lists:sort(LessFun, TopRows),
        [FirstGroup | RestGroups] = group_by_similar_keys(SortedRows, []),
        case FirstGroup of
        [Row] ->
            ok;
        [{K, _}, _ | _] ->
            RedVal = rereduce(FirstGroup, Params),
            Row = {K, RedVal}
        end,
        QueueMap2 = lists:foldl(
            fun(R, Acc) ->
                RQueues = dict:fetch(R, RowsToQueuesMap),
                lists:foldl(fun(Q, D) -> dict:erase(Q, D) end, Acc, RQueues)
            end,
            QueueMap,
            FirstGroup),
        QueueMap3 = lists:foldl(
            fun(R, Map) ->
                QList = dict:fetch(R, RowsToQueuesMap),
                lists:foldl(fun(Q, D) -> dict:store(Q, R, D) end, Map, QList)
            end,
            QueueMap2,
            lists:flatten(RestGroups)),
        case Skip > 0 of
        true ->
            Limit2 = Limit;
        false ->
            Col ! {row, Row},
            Limit2 = dec_counter(Limit)
        end,
        Params2 = Params#merge_params{
            queues = Queues2, queue_map = QueueMap3,
            skip = dec_counter(Skip), limit = Limit2
        },
        merge_red_views(Params2)
    end.


rereduce(Rows, #merge_params{view_lang = Lang, view_def = {ViewDef}}) ->
    RedFun = get_value(<<"reduce">>, ViewDef),
    Reds = [[Val] || {_Key, Val} <- Rows],
    {ok, [Value]} = couch_query_servers:rereduce(Lang, [RedFun], Reds),
    Value.


group_by_similar_keys([], Groups) ->
    lists:reverse(Groups);

group_by_similar_keys([Row | Rest], []) ->
    group_by_similar_keys(Rest, [[Row]]);

group_by_similar_keys([{K, _} = R | Rest], [[{K, _} | _] = Group | RestGroups]) ->
    group_by_similar_keys(Rest, [[R | Group] | RestGroups]);

group_by_similar_keys([Row | Rest], Groups) ->
    group_by_similar_keys(Rest, [[Row] | Groups]).


dec_counter(0) -> 0;
dec_counter(N) -> N - 1.


dequeue(Queues, QueueMap, Collector) ->
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
                    Collector ! RowCount,
                    case couch_work_queue:dequeue(Q, 1) of
                    {ok, [Row]} ->
                        {[Row | RowAcc], dict:append(Row, Q, RMap), Closed};
                    closed ->
                        {RowAcc, RMap, [Q | Closed]}
                    end;
                {ok, [{error, _DbUrl, _Reason} = Error]} ->
                    Collector ! {Error, self()},
                    receive
                    {continue, Collector} ->
                        {RowAcc, RMap, [Q | Closed]};
                    {stop, Resp, Collector} ->
                        throw({stop, Resp})
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


map_view_folder(<<"http://", _/binary>> = DbName, MergeParams, _UserCtx,
                DDocId, ViewName, Keys, ViewArgs, Queue) ->
    http_view_folder(
        DbName, MergeParams, DDocId, ViewName, Keys, ViewArgs, Queue);

map_view_folder(<<"https://", _/binary>> = DbName, MergeParams, _UserCtx,
                DDocId, ViewName, Keys, ViewArgs, Queue) ->
    http_view_folder(
        DbName, MergeParams, DDocId, ViewName, Keys, ViewArgs, Queue);

map_view_folder(DbName, _MergeParams, UserCtx, DDocId,
                ViewName, Keys, ViewArgs, Queue) ->
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


http_view_folder(DbUrl, MergeParams, DDocId, ViewName,
                 Keys, ViewArgs, Queue) ->
    {ok, #httpdb{url = Url, ibrowse_options = Options} = Db} =
        open_db(DbUrl, nil, MergeParams),
    Qs = case view_qs_params(ViewArgs) of
    [] ->
        "";
    QsParams ->
        "?" ++ string:join(QsParams, "&")
    end,
    ViewUrl = Url ++ ?b2l(DDocId) ++ "/_view/" ++ ?b2l(ViewName) ++
    case {ViewArgs#view_query_args.view_type, Qs} of
    {red_map, ""} ->
        "?reduce=false";
    {red_map, [$? | _]} ->
        Qs ++ "&reduce=false";
    _ ->
        Qs
    end,
    {Method, Headers, Body} = case Keys of
    nil ->
        {get, [], []};
    _ ->
        {post, [{"Content-Type", "application/json"}],
            ?JSON_ENCODE({[{<<"keys">>, Keys}]})}
    end,
    {ok, Conn} = ibrowse:spawn_link_worker_process(ViewUrl),
    {ibrowse_req_id, ReqId} = ibrowse:send_req_direct(
        Conn, ViewUrl, Headers ++ Db#httpdb.headers, Method, Body,
        [{stream_to, {self(), once}} | Options]),
    receive
    {ibrowse_async_headers, ReqId, "200", _RespHeaders} ->
        ibrowse:stream_next(ReqId),
        DataFun = fun() -> stream_data(ReqId) end,
        EventFun = fun(Ev) ->
            http_view_fold(Ev, ViewArgs#view_query_args.view_type, Queue)
        end,
        try
            json_stream_parse:events(DataFun, EventFun)
        catch throw:{error, Error} ->
            couch_work_queue:queue(Queue, {error, DbUrl, Error})
        after
            stop_conn(Conn),
            couch_work_queue:close(Queue)
        end;
    {ibrowse_async_headers, ReqId, Code, _RespHeaders} ->
        Body = stream_all(ReqId, []),
        Reason = {[{code, list_to_integer(Code)}, {reason, Body}]},
        stop_conn(Conn),
        couch_work_queue:queue(
            Queue, {error, DbUrl, {[{code, list_to_integer(Code)}]}}),
        couch_work_queue:close(Queue);
    {ibrowse_async_response, ReqId, {error, Error}} ->
        stop_conn(Conn),
        couch_work_queue:queue(Queue, {error, DbUrl, Error}),
        couch_work_queue:close(Queue)
    end.


stream_data(ReqId) ->
    receive
    {ibrowse_async_response, ReqId, {error, _} = Error} ->
        throw(Error);
    {ibrowse_async_response, ReqId, <<>>} ->
        ibrowse:stream_next(ReqId),
        stream_data(ReqId);
    {ibrowse_async_response, ReqId, Data} ->
        ibrowse:stream_next(ReqId),
        {Data, fun() -> stream_data(ReqId) end};
    {ibrowse_async_response_end, ReqId} ->
        {<<>>, fun() -> throw({error, <<"more view data expected">>}) end}
    end.


stream_all(ReqId, Acc) ->
    case stream_data(ReqId) of
    {<<>>, _} ->
        iolist_to_binary(lists:reverse(Acc));
    {Data, _} ->
        stream_all(ReqId, [Data | Acc])
    end.


http_view_fold(object_start, map, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end;
http_view_fold(object_start, red_map, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end;
http_view_fold(object_start, reduce, Queue) ->
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rc_1({key, <<"total_rows">>}, Queue) ->
    fun(Ev) -> http_view_fold_rc_2(Ev, Queue) end;
http_view_fold_rc_1(_Ev, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end.

http_view_fold_rc_2(RowCount, Queue) when is_number(RowCount) ->
    couch_work_queue:queue(Queue, {row_count, RowCount}),
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rows_1({key, <<"rows">>}, Queue) ->
    fun(array_start) -> fun(Ev) -> http_view_fold_rows_2(Ev, Queue) end end;
http_view_fold_rows_1(_Ev, Queue) ->
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rows_2(array_end, _Queue) ->
    fun void_event/1;
http_view_fold_rows_2(object_start, Queue) ->
    fun(Ev) ->
        json_stream_parse:collect_object(
            Ev,
            fun(Row) ->
                http_view_fold_queue_row(Row, Queue),
                fun(Ev2) -> http_view_fold_rows_2(Ev2, Queue) end
            end)
    end.

http_view_fold_queue_row({Props}, Queue) ->
    Id = get_value(<<"id">>, Props, nil),
    Key = get_value(<<"key">>, Props),
    Val = get_value(<<"value">>, Props),
    Row = case get_value(<<"doc">>, Props, nil) of
    nil ->
        case Id of
        nil ->
            % reduce view row
            {Key, Val};
        _ ->
            {{Key, Id}, Val}
        end;
    Doc ->
        {{Key, Id}, Val, {doc, Doc}}
    end,
    couch_work_queue:queue(Queue, Row).

void_event(_Ev) ->
    fun void_event/1.


red_view_folder(<<"http://", _/binary>> = DbName, MergeParams, _UserCtx,
                DDocId, ViewName, Keys, ViewArgs, Queue) ->
    http_view_folder(
        DbName, MergeParams, DDocId, ViewName, Keys, ViewArgs, Queue);

red_view_folder(<<"https://", _/binary>> = DbName, MergeParams, _UserCtx,
                DDocId, ViewName, Keys, ViewArgs, Queue) ->
    http_view_folder(
        DbName, MergeParams, DDocId, ViewName, Keys, ViewArgs, Queue);

red_view_folder(DbName, _MergeParams, UserCtx, DDocId, ViewName, Keys,
                ViewArgs, Queue) ->
    #view_query_args{
        stale = Stale
    } = ViewArgs,
    {ok, Db} = couch_db:open(DbName, [{user_ctx, UserCtx}]),
    try
        FoldlFun = make_red_fold_fun(ViewArgs, Queue),
        KeyGroupFun = make_group_rows_fun(ViewArgs),
        {ok, View, _} = couch_view:get_reduce_view(Db, DDocId, ViewName, Stale),
        case Keys of
        nil ->
            FoldOpts = [{key_group_fun, KeyGroupFun} |
                couch_httpd_view:make_key_options(ViewArgs)],
            {ok, _} = couch_view:fold_reduce(View, FoldlFun, [], FoldOpts);
        _ when is_list(Keys) ->
            lists:foreach(
                fun(K) ->
                    FoldOpts = [{key_group_fun, KeyGroupFun} |
                        couch_httpd_view:make_key_options(
                            ViewArgs#view_query_args{
                                start_key = K, end_key = K})],
                    {ok, _} = couch_view:fold_reduce(View, FoldlFun, [], FoldOpts)
                end,
                Keys)
        end,
        couch_work_queue:close(Queue)
    after
        couch_db:close(Db)
    end.


make_group_rows_fun(#view_query_args{group_level = 0}) ->
    fun(_, _) -> true end;

make_group_rows_fun(#view_query_args{group_level = L}) when is_integer(L) ->
    fun({KeyA, _}, {KeyB, _}) when is_list(KeyA) andalso is_list(KeyB) ->
        lists:sublist(KeyA, L) == lists:sublist(KeyB, L);
    ({KeyA, _}, {KeyB, _}) ->
        KeyA == KeyB
    end;

make_group_rows_fun(_) ->
    fun({KeyA, _}, {KeyB, _}) -> KeyA == KeyB end.


make_red_fold_fun(#view_query_args{group_level = 0}, Queue) ->
    fun(_Key, Red, Acc) ->
        couch_work_queue:queue(Queue, {null, Red}),
        {ok, Acc}
    end;

make_red_fold_fun(#view_query_args{group_level = L}, Queue) when is_integer(L) ->
    fun(Key, Red, Acc) when is_list(Key) ->
        couch_work_queue:queue(Queue, {lists:sublist(Key, L), Red}),
        {ok, Acc};
    (Key, Red, Acc) ->
        couch_work_queue:queue(Queue, {Key, Red}),
        {ok, Acc}
    end;

make_red_fold_fun(_QueryArgs, Queue) ->
    fun(Key, Red, Acc) ->
        couch_work_queue:queue(Queue, {Key, Red}),
        {ok, Acc}
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


check_view_exists([], _UserCtx, _MergeParams, DDoc) ->
    DDoc;
check_view_exists([DbName | Rest], UserCtx, MergeParams, DDoc) ->
    #view_merge{ddoc_id = DDocId, view_name = ViewName} = MergeParams,
    {ok, Db} = open_db(DbName, UserCtx, MergeParams),
    {ok, #doc{body = ThisDDoc}} = get_ddoc(Db, DDocId),
    close_db(Db),
    try
        get_nested_json_value(ThisDDoc, [<<"views">>, ViewName])
    catch throw:_ ->
        throw({<<"missing_view_in_db">>, db_uri(Db)})
    end,
    case DDoc of
    nil ->
        check_view_exists(Rest, UserCtx, MergeParams, ThisDDoc);
    ThisDDoc ->
        check_view_exists(Rest, UserCtx, MergeParams, DDoc);
    _ ->
        throw({<<"ddocs_dont_match">>, DDocId, ViewName})
    end.


open_db(<<"http://", _/binary>> = DbName, _UserCtx, MergeParams) ->
    HttpDb = #httpdb{
        url = maybe_add_trailing_slash(DbName),
        timeout = MergeParams#view_merge.conn_timeout
    },
    {ok, HttpDb#httpdb{ibrowse_options = ibrowse_options(HttpDb)}};
open_db(<<"https://", _/binary>> = DbName, _UserCtx, MergeParams) ->
    HttpDb = #httpdb{
        url = maybe_add_trailing_slash(DbName),
        timeout = MergeParams#view_merge.conn_timeout
    },
    {ok, HttpDb#httpdb{ibrowse_options = ibrowse_options(HttpDb)}};
open_db(DbName, UserCtx, _MergeParams) ->
    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
    {ok, _} = Ok ->
        Ok;
    Error ->
        throw({<<"db_open_error">>, DbName, Error})
    end.


close_db(#httpdb{}) ->
    ok;
close_db(Db) ->
    couch_db:close(Db).


get_ddoc(#httpdb{url = BaseUrl, headers = Headers} = HttpDb, Id) ->
    Url = BaseUrl ++ ?b2l(Id),
    case ibrowse:send_req(
        Url, Headers, get, [], HttpDb#httpdb.ibrowse_options) of
    {ok, "200", _RespHeaders, Body} ->
        {ok, couch_doc:from_json_obj(?JSON_DECODE(Body))};
    {ok, _Code, _RespHeaders, Body} ->
        {error, Body};
    Error ->
        Error
    end;
get_ddoc(Db, Id) ->
    case couch_api_wrap:open_doc(Db, Id, [ejson_body]) of
    {ok, _} = Ok ->
        Ok;
    {error, Error} ->
        throw({<<"ddoc_open_error">>, db_uri(Db), Error})
    end.


db_uri(#httpdb{url = Url}) ->
    ?l2b(couch_util:url_strip_password(Url));
db_uri(#db{name = Name}) ->
    Name.


maybe_add_trailing_slash(Url) when is_binary(Url) ->
    maybe_add_trailing_slash(?b2l(Url));
maybe_add_trailing_slash(Url) ->
    case lists:last(Url) of
    $/ ->
        Url;
    _ ->
        Url ++ "/"
    end.


ibrowse_options(#httpdb{timeout = T, url = Url}) ->
    [{inactivity_timeout, T}, {connect_timeout, T},
        {response_format, binary}] ++
    case Url of
    <<"https://", _/binary>> ->
        % TODO: add SSL options like verify and cacertfile
        [{is_ssl, true}];
    _ ->
        []
    end.


view_qs_params(ViewArgs) ->
    DefViewArgs = #view_query_args{},
    #view_query_args{
        start_key = StartKey, end_key = EndKey,
        start_docid = StartDocId, end_docid = EndDocId,
        direction = Dir,
        inclusive_end = IncEnd,
        group_level = GroupLevel,
        view_type = ViewType,
        include_docs = IncDocs,
        conflicts = Conflicts,
        stale = Stale
    } = ViewArgs,
    case StartKey =:= DefViewArgs#view_query_args.start_key of
    true ->
        [];
    false ->
        ["startkey=" ++ json_qs_val(StartKey)]
    end ++
    case EndKey =:= DefViewArgs#view_query_args.end_key of
    true ->
        [];
    false ->
        ["endkey=" ++ json_qs_val(EndKey)]
    end ++
    case {Dir, StartDocId =:= DefViewArgs#view_query_args.start_docid} of
    {fwd, false} ->
        ["startkey_docid=" ++ ?b2l(StartDocId)];
    _ ->
        []
    end ++
    case {Dir, EndDocId =:= DefViewArgs#view_query_args.end_docid} of
    {fwd, false} ->
        ["endkey_docid=" ++ ?b2l(EndDocId)];
    _ ->
        []
    end ++
    case Dir of
    fwd ->
        [];
    rev ->
        StartDocId1 = reverse_key_default(StartDocId),
        EndDocId1 = reverse_key_default(EndDocId),
        ["descending=true"] ++
        case StartDocId1 =:= DefViewArgs#view_query_args.start_docid of
        true ->
            [];
        false ->
            ["startkey_docid=" ++ json_qs_val(StartDocId1)]
        end ++
        case EndDocId1 =:= DefViewArgs#view_query_args.end_docid of
        true ->
            [];
        false ->
            ["endkey_docid=" ++ json_qs_val(EndDocId1)]
        end
    end ++
    case IncEnd =:= DefViewArgs#view_query_args.inclusive_end of
    true ->
        [];
    false ->
        ["inclusive_end=" ++ atom_to_list(IncEnd)]
    end ++
    case GroupLevel =:= DefViewArgs#view_query_args.group_level of
    true ->
        [];
    false ->
        case GroupLevel of
        exact ->
            ["group=true"];
        _ when is_number(GroupLevel) ->
            ["group_level=" ++ integer_to_list(GroupLevel)]
        end
    end ++
    case ViewType of
    red_map ->
        ["reduce=false"];
    _ ->
        []
    end ++
    case IncDocs =:= DefViewArgs#view_query_args.include_docs of
    true ->
        [];
    false ->
        ["include_docs=" ++ atom_to_list(IncDocs)]
    end ++
    case Conflicts =:= DefViewArgs#view_query_args.conflicts of
    true ->
        [];
    false ->
        ["conflicts=" ++ atom_to_list(Conflicts)]
    end ++
    case Stale =:= DefViewArgs#view_query_args.stale of
    true ->
        [];
    false ->
        ["stale=" ++ atom_to_list(Stale)]
    end.

json_qs_val(Value) ->
    couch_httpd:quote(?b2l(iolist_to_binary(?JSON_ENCODE(Value)))).

reverse_key_default(?MIN_STR) -> ?MAX_STR;
reverse_key_default(?MAX_STR) -> ?MIN_STR;
reverse_key_default(Key) -> Key.


stop_conn(Conn) ->
    unlink(Conn),
    receive {'EXIT', Conn, _} -> ok after 0 -> ok end,
    catch ibrowse:stop_worker_process(Conn).
