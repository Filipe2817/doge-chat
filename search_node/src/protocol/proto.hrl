%%--------------------------------------------------------------------
%% Key‑Value protocol – packet records & macros
%%--------------------------------------------------------------------
-ifndef(KV_PROTO_HRL).
-define(KV_PROTO_HRL, true).

%% ---- type / status tags in JSON -----------------------------------
-define(TYPE_GET,          <<"get">>).
-define(TYPE_GET_RESP,     <<"get_response">>).
-define(TYPE_SET,          <<"set">>).
-define(TYPE_SET_RESP,     <<"set_response">>).

-define(ST_OK,             <<"ok">>).
-define(ST_ERROR,          <<"error">>).
-define(ST_NOT_FOUND,      <<"not_found">>).

%% ---- packet structs (records) -------------------------------------
-record(get, {
    is_peer :: boolean(),
    key :: binary()
}).

-record(get_response, {
    status :: ok | error | not_found,
    key    :: binary(),
    value  :: binary() | undefined   %% undefined if not_found / error
}).

-record(set, {
    is_peer :: boolean(),
    key     :: binary(),
    value   :: binary()
}).

-record(set_response, {
    status :: ok | error,
    key    :: binary(),
    value  :: binary()
}).

%% Join and replicate

-endif.
