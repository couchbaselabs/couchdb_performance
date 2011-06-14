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

-module(couch_httpd_cluster_view).

-export([handle_req/1]).

-include("couch_db.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3
]).


handle_req(#httpd{method = 'GET'} = Req) ->
    Dbs = validate_databases_param(couch_httpd:qs_json_value(Req, "databases")),
    {DDocId, ViewName} = validate_viewname_param(
        couch_httpd:qs_json_value(Req, "viewname")),
    Keys = validate_keys_param(couch_httpd:qs_json_value(Req, "keys", nil)),
    couch_cluster_view:query_view(DDocId, ViewName, Dbs, Keys, Req);

handle_req(#httpd{method = 'POST'} = Req) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {Props} = couch_httpd:json_body_obj(Req),
    Dbs = validate_databases_param(get_value(<<"databases">>, Props)),
    {DDocId, ViewName} = validate_viewname_param(
        get_value(<<"viewname">>, Props)),
    Keys = validate_keys_param(get_value(<<"keys">>, Props, nil)),
    couch_cluster_view:query_view(DDocId, ViewName, Dbs, Keys, Req);

handle_req(Req) ->
    couch_httpd:send_method_not_allowed(Req, "GET,POST").


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
