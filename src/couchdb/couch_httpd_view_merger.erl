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

-module(couch_httpd_view_merger).

-export([handle_req/1]).

-include("couch_db.hrl").
-include("couch_view_merger.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3,
    to_binary/1
]).

-record(sender_acc, {
    req = nil,
    resp = nil,
    on_error,
    acc = <<>>,
    error_acc = []
}).


handle_req(#httpd{method = 'GET'} = Req) ->
    Dbs = validate_databases_param(couch_httpd:qs_json_value(Req, "databases")),
    {DDocId, ViewName} = validate_viewname_param(
        couch_httpd:qs_json_value(Req, "viewname")),
    Keys = validate_keys_param(couch_httpd:qs_json_value(Req, "keys", nil)),
    MergeParams0 = #view_merge{
        ddoc_id = DDocId,
        view_name = ViewName,
        databases = Dbs,
        keys = Keys,
        callback = fun http_sender/2
    },
    MergeParams1 = apply_http_config(Req, [], MergeParams0),
    MergeParams2 = MergeParams1#view_merge{
        user_acc = #sender_acc{
            req = Req, on_error = MergeParams1#view_merge.on_error
        }
    },
    couch_view_merger:query_view(Req, MergeParams2);

handle_req(#httpd{method = 'POST'} = Req) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {Props} = couch_httpd:json_body_obj(Req),
    Dbs = validate_databases_param(get_value(<<"databases">>, Props)),
    {DDocId, ViewName} = validate_viewname_param(
        get_value(<<"viewname">>, Props)),
    Keys = validate_keys_param(get_value(<<"keys">>, Props, nil)),
    MergeParams0 = #view_merge{
        ddoc_id = DDocId,
        view_name = ViewName,
        databases = Dbs,
        keys = Keys,
        callback = fun http_sender/2
    },
    MergeParams1 = apply_http_config(Req, Props, MergeParams0),
    MergeParams2 = MergeParams1#view_merge{
        user_acc = #sender_acc{
            req = Req, on_error = MergeParams1#view_merge.on_error
        }
    },
    couch_view_merger:query_view(Req, MergeParams2);

handle_req(Req) ->
    couch_httpd:send_method_not_allowed(Req, "GET,POST").


apply_http_config(Req, Body, MergeParams) ->
    DefConnTimeout = MergeParams#view_merge.conn_timeout,
    ConnTimeout = case get_value(<<"connection_timeout">>, Body, nil) of
    nil ->
        couch_httpd:qs_json_value(Req, "connection_timeout", DefConnTimeout);
    T when is_integer(T) ->
        T
    end,
    OnError = case get_value(<<"connection_timeout">>, Body, <<"continue">>) of
    <<"continue">> ->
        continue;
    <<"stop">> ->
        stop
    end,
    MergeParams#view_merge{conn_timeout = ConnTimeout, on_error = OnError}.


http_sender(start, #sender_acc{req = Req, error_acc = ErrorAcc} = SAcc) ->
    {ok, Resp} = couch_httpd:start_json_response(Req, 200, []),
    couch_httpd:send_chunk(Resp, <<"{\"rows\":[">>),
    case ErrorAcc of
    [] ->
        Acc = <<"\r\n">>;
    _ ->
        lists:foreach(
            fun(Row) -> couch_httpd:send_chunk(Resp, [Row, <<",\r\n">>]) end,
            lists:reverse(ErrorAcc)),
        Acc = <<>>
    end,
    {ok, SAcc#sender_acc{resp = Resp, acc = Acc}};

http_sender({start, RowCount}, #sender_acc{req = Req, error_acc = ErrorAcc} = SAcc) ->
    Start = io_lib:format(
        "{\"total_rows\":~w,\"rows\":[", [RowCount]),
    {ok, Resp} = couch_httpd:start_json_response(Req, 200, []),
    couch_httpd:send_chunk(Resp, Start),
    case ErrorAcc of
    [] ->
        Acc = <<"\r\n">>;
    _ ->
        lists:foreach(
            fun(Row) -> couch_httpd:send_chunk(Resp, [Row, <<",\r\n">>]) end,
            lists:reverse(ErrorAcc)),
        Acc = <<>>
    end,
    {ok, SAcc#sender_acc{resp = Resp, acc = Acc, error_acc = []}};

http_sender({row, Row}, #sender_acc{resp = Resp, acc = Acc} = SAcc) ->
    couch_httpd:send_chunk(Resp, [Acc, ?JSON_ENCODE(Row)]),
    {ok, SAcc#sender_acc{acc = <<",\r\n">>}};

http_sender(stop, #sender_acc{resp = Resp}) ->
    couch_httpd:send_chunk(Resp, <<"\r\n]}">>),
    {ok, couch_httpd:end_json_response(Resp)};

http_sender({error, DbUrl, Reason}, #sender_acc{on_error = continue} = SAcc) ->
    #sender_acc{resp = Resp, error_acc = ErrorAcc, acc = Acc} = SAcc,
    Row = {[
        {<<"error">>, true}, {<<"database">>, to_binary(DbUrl)},
        {<<"reason">>, to_binary(Reason)}
    ]},
    case Resp of
    nil ->
        % we haven't started the response yet
        ErrorAcc2 = [?JSON_ENCODE(Row) | ErrorAcc],
        Acc2 = Acc;
    _ ->
        couch_httpd:send_chunk(Resp, [Acc, ?JSON_ENCODE(Row)]),
        ErrorAcc2 = ErrorAcc,
        Acc2 = <<",\r\n">>
    end,
    {ok, SAcc#sender_acc{error_acc = ErrorAcc2, acc = Acc2}};

http_sender({error, DbUrl, Reason}, #sender_acc{on_error = stop} = SAcc) ->
    #sender_acc{req = Req, resp = Resp, acc = Acc} = SAcc,
    Row = {[
        {<<"error">>, true}, {<<"database">>, to_binary(DbUrl)},
        {<<"reason">>, to_binary(Reason)}
    ]},
    case Resp of
    nil ->
        % we haven't started the response yet
        Start = io_lib:format("{\"total_rows\":~w,\"rows\":[\r\n", [0]),
        {ok, Resp2} = couch_httpd:start_json_response(Req, 200, []),
        couch_httpd:send_chunk(Resp2, Start),
        couch_httpd:send_chunk(Resp2, ?JSON_ENCODE(Row)),
        couch_httpd:send_chunk(Resp2, <<"\r\n]}">>);
    _ ->
       Resp2 = Resp,
       couch_httpd:send_chunk(Resp2, [Acc, ?JSON_ENCODE(Row)]),
       couch_httpd:send_chunk(Resp2, <<"\r\n]}">>)
    end,
    {stop, Resp2}.


validate_databases_param([_Db1 | _Rest] = Dbs) ->
    Dbs;
validate_databases_param(_) ->
    throw({bad_request, <<"`databases` parameter must be an array with at ",
                          "least 1 database names.">>}).


validate_viewname_param(Bin) when is_binary(Bin) ->
    case string:tokens(couch_util:trim(?b2l(Bin)), "/") of
    [DDocName, ViewName] ->
        {<<"_design/", (?l2b(DDocName))/binary>>, ?l2b(ViewName)};
    ["_design", DDocName, ViewName] ->
        {<<"_design/", (?l2b(DDocName))/binary>>, ?l2b(ViewName)};
    _ ->
        throw({bad_request, <<"`viewname` parameter must be a string with the ",
                              "format ddocname/viewname.">>})
    end;
validate_viewname_param(_) ->
    throw({bad_request, <<"`viewname` parameter must be a string with the ",
                          "format ddocname/viewname.">>}).


validate_keys_param(nil) ->
    nil;
validate_keys_param(Keys) when is_list(Keys) ->
    Keys;
validate_keys_param(_) ->
    throw({bad_request, "`keys` parameter is not an array."}).
