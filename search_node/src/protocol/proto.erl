-module(proto).
-include("proto.hrl").           %% only this module needs the header

-export([
    % smart constructors
    get/1,
    get/2,
    get_resp/3,
    get_resp/4,
    set/2,
    set/3,
    set_resp/3,
    set_resp/4,
    join_init/3,
    join_init_resp/2,
    join_get_keys/2,
    join_get_keys_resp/1,
    join_ready/4,
    % predicates (optional)
    is_get/1,
    is_get_resp/1,
    is_set/1,
    is_set_resp/1,
    is_join_init/1,
    is_join_init_resp/1,
    is_join_get_keys/1,
    is_join_get_keys_resp/1,
    is_join_ready/1
]).
-export_type([
    get/0, 
    get_response/0, 
    set/0,
    set_response/0,
    join_init/0,
    join_init_response/0,
    join_get_keys/0,
    join_get_keys_response/0,
    join_ready/0
]).

%%--------------------------------------------------------------------
%% Type aliases so users can dialyze without the header
%%--------------------------------------------------------------------

-type get()                     :: #get{}.
-type get_response()            :: #get_response{}.
-type set()                     :: #set{}.
-type set_response()            :: #set_response{}.
-type join_init()               :: #join_init{}.
-type join_init_response()      :: #join_init_response{}.
-type join_get_keys()           :: #join_get_keys{}.
-type join_get_keys_response()  :: #join_get_keys_response{}.
-type join_ready()              :: #join_ready{}.

%%--------------------------------------------------------------------
%% Smart constructors
%%--------------------------------------------------------------------

-spec get(binary() | string()) -> get().
get(Key) when is_list(Key) ->
    #get{key = list_to_binary(Key)};
get(Key) ->
    #get{key = Key}.

-spec get(binary() | string(), term()) -> get().
get(Key, Ref) when is_list(Key) ->
    #get{key = list_to_binary(Key), ref = Ref};
get(Key, Ref) ->
    #get{key = Key, ref = Ref}.

-spec get_resp(ok | error | not_found, binary(), binary() | undefined) -> get_response().
get_resp(Status, Key, Val) when Status == ok; Status == error; Status == not_found ->
    #get_response{status = Status, key = Key, value = Val}.

-spec get_resp(ok | error | not_found, binary(), binary() | undefined, term()) -> get_response().
get_resp(Status, Key, Val, Ref) when Status == ok; Status == error; Status == not_found ->
    #get_response{status = Status, key = Key, value = Val, ref = Ref}.

%%%%%%%%%%%%

-spec set(binary(), binary()) -> set().
set(Key, Val) ->
    #set{key = Key, value = Val}.

-spec set(binary(), binary(), term()) -> set().
set(Key, Val, Ref) ->
    #set{key = Key, value = Val, ref = Ref}.

-spec set_resp(ok | error, binary(), binary()) -> set_response().
set_resp(Status, Key, Val) when Status == ok; Status == error ->
    #set_response{status = Status, key = Key, value = Val}.

-spec set_resp(ok | error, binary(), binary(), term()) -> set_response().
set_resp(Status, Key, Val, Ref) when Status == ok; Status == error ->
    #set_response{status = Status, key = Key, value = Val, ref = Ref}.

%%%%%%%%%%%%

-spec join_init(binary(), binary(), binary()) -> join_init().
join_init(NodeId, Addr, Port) ->
    #join_init{node_id = NodeId, address = Addr, port = Port}.

-spec join_init_resp([{binary(), binary(), binary()}], [{binary(), binary()}]) -> join_init_response().
join_init_resp(Nodes, Hashes) ->
    #join_init_response{nodes = Nodes, hashes = Hashes}.

%%%%%%%%%%%%

-spec join_get_keys(binary(), [{binary(), binary()}]) -> join_get_keys().
join_get_keys(NodeId, HashRanges) ->
    #join_get_keys{node_id = NodeId, hash_ranges = HashRanges}.

-spec join_get_keys_resp(map()) -> join_get_keys_response().
join_get_keys_resp(TransferData) when is_map(TransferData) ->
    #join_get_keys_response{transfer_map = TransferData}.

%%%%%%%%%%%%

-spec join_ready(binary(), binary(), binary(), [{binary(), binary()}]) -> join_ready().
join_ready(NodeId, Addr, Port, Hashes) ->
    #join_ready{node_id = NodeId, address = Addr, port = Port, hashes = Hashes}.

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

-spec is_join_init(term()) -> boolean().
is_join_init(#join_init{}) -> true; is_join_init(_) -> false.

-spec is_join_init_resp(term()) -> boolean().
is_join_init_resp(#join_init_response{}) -> true; is_join_init_resp(_) -> false.

-spec is_join_get_keys(term()) -> boolean().
is_join_get_keys(#join_get_keys{}) -> true; is_join_get_keys(_) -> false.

-spec is_join_get_keys_resp(term()) -> boolean().
is_join_get_keys_resp(#join_get_keys_response{}) -> true; is_join_get_keys_resp(_) -> false.

-spec is_join_ready(term()) -> boolean().
is_join_ready(#join_ready{}) -> true; is_join_ready(_) -> false.
