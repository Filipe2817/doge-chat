-module(proto).
-include("proto.hrl").           %% only this module needs the header

-export([
    % smart constructors
    get/2,
    get_resp/3,
    set/3,
    set_resp/3,
    % predicates (optional)
    is_get/1,
    is_get_resp/1,
    is_set/1,
    is_set_resp/1
]).
-export_type([
    get/0, 
    get_response/0, 
    set/0,
    set_response/0 
]).

%%--------------------------------------------------------------------
%% Type aliases so users can dialyze without the header
%%--------------------------------------------------------------------

-type get()          :: #get{}.
-type get_response() :: #get_response{}.
-type set()          :: #set{}.
-type set_response() :: #set_response{}.

%%--------------------------------------------------------------------
%% Smart constructors
%%--------------------------------------------------------------------

-spec get(boolean(), binary() | string()) -> get().
get(IsPeer, Key) when is_list(Key) ->
    #get{is_peer = IsPeer, key = list_to_binary(Key)};
get(IsPeer, Key) ->
    #get{is_peer = IsPeer, key = Key}.

-spec get_resp(ok | error | not_found, binary(), binary() | undefined) -> get_response().
get_resp(Status, Key, Val) when Status == ok; Status == error; Status == not_found ->
    #get_response{status = Status, key = Key, value = Val}.

-spec set(boolean(), binary(), binary()) -> set().
set(IsPeer, Key, Val) ->
    #set{is_peer = IsPeer, key = Key, value = Val}.

-spec set_resp(ok | error, binary(), binary()) -> set_response().
set_resp(Status, Key, Val) when Status == ok; Status == error ->
    #set_response{status = Status, key = Key, value = Val}.

%%--------------------------------------------------------------------
%% Predicates (pattern‑matching helpers)
%%--------------------------------------------------------------------

-spec is_get(term()) -> boolean().
is_get(#get{}) -> true; is_get(_) -> false.

-spec is_get_resp(term()) -> boolean().
is_get_resp(#get_response{}) -> true; is_get_resp(_) -> false.

-spec is_set(term()) -> boolean().
is_set(#set{}) -> true; is_set(_) -> false.

-spec is_set_resp(term()) -> boolean().
is_set_resp(#set_response{}) -> true; is_set_resp(_) -> false.
