%%--------------------------------------------------------------------
%% Key‑Value protocol – packet records & macros
%%--------------------------------------------------------------------
-ifndef(KV_PROTO_HRL).
-define(KV_PROTO_HRL, true).

%% ---- type / status tags in JSON -----------------------------------
-define(TYPE_GET,               <<"get">>).
-define(TYPE_GET_RESP,          <<"get_response">>).
-define(TYPE_SET,               <<"set">>).
-define(TYPE_SET_RESP,          <<"set_response">>).
-define(TYPE_JOIN_INIT,         <<"join_init">>).
-define(TYPE_JOIN_INIT_RESP,    <<"join_init_response">>).
-define(TYPE_JOIN_GET_KEYS,     <<"join_get_keys">>).
-define(TYPE_JOIN_GET_KEYS_RESP,<<"join_get_keys_response">>).
-define(TYPE_JOIN_READY,        <<"join_ready">>).

-define(ST_OK,              <<"ok">>).
-define(ST_ERROR,           <<"error">>).
-define(ST_NOT_FOUND,       <<"not_found">>).

%% ---- packet structs (records) -------------------------------------
-record(get, {
    key :: binary(),
    ref = undefined
}).

-record(get_response, {
    status :: ok | error | not_found,
    key    :: binary(),
    value  :: binary() | undefined, %% undefined if not_found / error
    ref    = undefined
}).

-record(set, {
    key     :: binary(),
    value   :: binary(),
    ref     = undefined
}).

-record(set_response, {
    status :: ok | error,
    key    :: binary(),
    value  :: binary(),
    ref    = undefined
}).

-record(join_init, {
    node_id :: binary(),
    address :: binary(),
    port    :: binary()
}).

-record(join_init_response, {
    nodes  :: [{binary(), binary(), binary()}], % id, addr, port
    hashes :: [{binary(), binary()}]            % hash, id
}).

-record(join_get_keys, {
    node_id     :: binary(),
    hash_ranges :: [{binary(), binary()}] % [inital hash (exclusive), last hash (inclusive)]
}).

-record(join_get_keys_response, {
    transfer_map :: map()
}).

-record(join_ready, {
    node_id :: binary(),
    address :: binary(),
    port    :: binary(),
    hashes :: [{binary(), binary()}] % hash, id
}).

%% replicate

-endif.
