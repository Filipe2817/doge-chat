-module(proto).
-include("proto.hrl").           %% only this module needs the header

-export([
    % smart constructors
    get/1,
    get_resp/3,
    set/2,
	set_response/3,

    % predicates (optional)
    is_get/1,
    is_set/1,
    is_get_resp/1,
	is_set_resp/1
]).
-export_type([
    get/0, get_response/0, set/0, set_response/0
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

-spec get(binary() | string()) -> get().
get(Key) when is_list(Key) ->
    #get{key = list_to_binary(Key)};
get(Key) ->
    #get{key = Key}.

-spec get_resp(ok | error | not_found, binary(), binary()) -> get_response().
get_resp(Status, Key, Val) when Status == ok; Status == error; Status == not_found ->
    #get_response{status = Status, key = Key, value = Val}.

-spec set(binary(), binary()) -> set().
set(Key, Val) ->
    #set{key = Key, value = Val}.

-spec set_response(ok | error, binary(), binary()) -> set_response().
set_response(Status, Key, Val) when Status == ok; Status == error ->
	#set_response{status = Status, key = Key, value = Val}.

%%--------------------------------------------------------------------
%% Predicates (pattern‑matching helpers)
%%--------------------------------------------------------------------
-spec is_get(term())          -> boolean().
is_get(#get{}) -> true; is_get(_) -> false.

-spec is_set(term())          -> boolean().
is_set(#set{}) -> true; is_set(_) -> false.

-spec is_get_resp(term())     -> boolean().
is_get_resp(#get_response{}) -> true; is_get_resp(_) -> false.

-spec is_set_resp(term())	 -> boolean().
is_set_resp(#set_response{}) -> true; is_set_resp(_) -> false.
