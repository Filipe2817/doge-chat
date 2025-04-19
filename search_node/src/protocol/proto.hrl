%%--------------------------------------------------------------------
%% Key‑Value protocol – packet records & macros
%%--------------------------------------------------------------------
-ifndef(KV_PROTO_HRL).
-define(KV_PROTO_HRL, true).

%% ---- type / status tags in JSON -----------------------------------
-define(TYPE_GET,          <<"get">>).
-define(TYPE_GET_RESP,     <<"get_response">>).
-define(TYPE_SET,          <<"set">>).

-define(ST_OK,             <<"ok">>).
-define(ST_ERROR,          <<"error">>).
-define(ST_NOT_FOUND,      <<"not_found">>).

-define(CT_CLIENT,         <<"client">>).   %% client_type
-define(CT_PEER,           <<"peer">>).

%% ---- packet structs (records) -------------------------------------
-record(get, {
          key :: binary()              %% <<"mykey">>
         }).

-record(get_response, {
          status :: ok | error | not_found,
          key    :: binary(),
          value  :: binary() | undefined   %% undefined if not_found / error
         }).

-record(set, {
          client_type :: client | peer,
          key         :: binary(),
          value       :: binary()
         }).

-endif.
