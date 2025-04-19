-module(codec).
-include("proto.hrl").

-export([encode/1, decode/1]).

%%====================================================================
%% Public API
%%====================================================================

-spec encode(#get{} | #get_response{} | #set{}) -> iodata().
encode(Struct) ->
    Map = struct_to_map(Struct),
	json:encode(Map).

-spec decode(binary() | iolist()) ->
          #get{} | #get_response{} | #set{} | no_return().
decode(Bin) ->
	Map = json:decode(iolist_to_binary(Bin)),
    map_to_struct(Map).


%%====================================================================
%% Private – struct → map
%%====================================================================

struct_to_map(#get{key = Key}) ->
    #{<<"type">> => ?TYPE_GET,
      <<"data">> => #{<<"key">> => Key}};

struct_to_map(#get_response{status = Status, key = Key, value = Val}) ->
    #{<<"type">>   => ?TYPE_GET_RESP,
      <<"status">> => status_bin(Status),
      <<"data">>   => #{<<"key">>   => Key,
                        <<"value">> => Val}};

struct_to_map(#set{client_type = CT, key = Key, value = Val}) ->
    #{<<"type">>        => ?TYPE_SET,
      <<"client_type">> => ct_bin(CT),
      <<"data">>        => #{<<"key">>   => Key,
                             <<"value">> => Val}}.

%%====================================================================
%% Private – map → struct
%%====================================================================

map_to_struct(#{<<"type">> := ?TYPE_GET,
                <<"data">> := #{<<"key">> := Key}}) ->
    #get{key = Key};

map_to_struct(#{<<"type">>   := ?TYPE_GET_RESP,
                <<"status">> := StBin,
                <<"data">>   := #{<<"key">> := Key, <<"value">> := Val}}) ->
    #get_response{status = status_atom(StBin),
                  key    = Key,
                  value  = Val};

map_to_struct(#{<<"type">>        := ?TYPE_SET,
                <<"client_type">> := CTBin,
                <<"data">>        := #{<<"key">> := Key, <<"value">> := Val}}) ->
    #set{client_type = ct_atom(CTBin),
         key         = Key,
         value       = Val};

map_to_struct(Unknown) ->
    error({unknown_packet, Unknown}).

%%====================================================================
%% Helpers – enum <-> binary
%%====================================================================

status_bin(ok)        -> ?ST_OK;
status_bin(error)     -> ?ST_ERROR;
status_bin(not_found) -> ?ST_NOT_FOUND.

status_atom(?ST_OK)        -> ok;
status_atom(?ST_ERROR)     -> error;
status_atom(?ST_NOT_FOUND) -> not_found.

ct_bin(client) -> ?CT_CLIENT;
ct_bin(peer)   -> ?CT_PEER.

ct_atom(?CT_CLIENT) -> client;
ct_atom(?CT_PEER)   -> peer.
