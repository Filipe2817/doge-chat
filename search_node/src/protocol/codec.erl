-module(codec).
-include("proto.hrl").

-export([encode/1, decode/1]).

%%====================================================================
%% Public API
%%====================================================================

-spec encode(
    #get{} | #get_response{} | 
    #set{} | #set_response{} | 
    #join_init{} | #join_init_response{} |
    #join_get_keys{} | #join_get_keys_response{} |
    #join_ready{}) -> iodata().
encode(Struct) ->
    Map = struct_to_map(Struct),
	json:encode(Map).

-spec decode(binary() | iolist()) -> 
    #get{} | #get_response{} | 
    #set{} | #set_response{} | 
    #join_init{} | #join_init_response{} |
    #join_get_keys{} | #join_get_keys_response{} |
    #join_ready{} | no_return().
decode(Bin) ->
	Map = json:decode(iolist_to_binary(Bin)),
    map_to_struct(Map).

%%====================================================================
%% Private – struct → map
%%====================================================================

struct_to_map(#get{key = Key, ref = Ref}) ->
    BaseData = #{<<"key">> => Key},
    #{
        <<"type">> => ?TYPE_GET,
        <<"data">> => try_encode_ref(BaseData, Ref)
    };

struct_to_map(#get_response{status = Status, key = Key, value = Val, ref = Ref}) ->
    BaseData = #{
        <<"key">> => Key,
        <<"value">> => Val
    },
    #{
        <<"type">> => ?TYPE_GET_RESP,
        <<"status">> => status_bin(Status),
        <<"data">> => try_encode_ref(BaseData, Ref)
    };

struct_to_map(#set{key = Key, value = Val, ref = Ref}) ->
    BaseData = #{
        <<"key">> => Key,
        <<"value">> => Val
    },
    #{
        <<"type">> => ?TYPE_SET,
        <<"data">> => try_encode_ref(BaseData, Ref)
    };

struct_to_map(#set_response{status = Status, key = Key, value = Val, ref = Ref}) ->
    BaseData = #{
        <<"key">> => Key,
        <<"value">> => Val
    },
    #{
        <<"type">> => ?TYPE_SET_RESP,
        <<"status">> => status_bin(Status),
        <<"data">> => try_encode_ref(BaseData, Ref)
    };

struct_to_map(#join_init{node_id = NodeId, address = NodeAddr, port = NodePort}) ->
    #{
        <<"type">> => ?TYPE_JOIN_INIT,
        <<"data">> => #{
            <<"node_id">> => atom_to_binary(NodeId, utf8),
            <<"node_addr">> => list_to_binary(inet:ntoa(NodeAddr)),
            <<"node_port">> => NodePort
        }
    };

struct_to_map(#join_init_response{nodes = Nodes, hashes = Hashes}) ->
    #{
        <<"type">> => ?TYPE_JOIN_INIT_RESP,
        <<"data">> => #{
            <<"nodes">> => [
                #{
                    <<"node_id">> => atom_to_binary(NodeId, utf8),
                    <<"node_addr">> => list_to_binary(inet:ntoa(NodeAddr)),
                    <<"node_port">> => NodePort
                } || {NodeId, NodeAddr, NodePort} <- Nodes
            ],
            <<"hashes">> => [
                #{
                    <<"hash">> => Hash, 
                    <<"node">> => atom_to_binary(Node, utf8)
                } || {Hash, Node} <- Hashes
            ]
        }
    };

struct_to_map(#join_get_keys{node_id = NodeId, hash_ranges = HasheRanges}) ->
    #{
        <<"type">> => ?TYPE_JOIN_GET_KEYS,
        <<"data">> => #{
            <<"node_id">> => atom_to_binary(NodeId, utf8),
            <<"hashes">> => [
                #{
                    <<"start">> => InitialHash,
                    <<"end">> => LastHash
                } || {InitialHash, LastHash} <- HasheRanges
            ]
        }
    };

struct_to_map(#join_get_keys_response{transfer_map = TransferData}) ->
    #{
        <<"type">> => ?TYPE_JOIN_GET_KEYS_RESP,
        <<"data">> => TransferData
    };

struct_to_map(#join_ready{node_id = NodeId, address = NodeAddr, port = NodePort, hashes = Hashes}) ->
    #{
        <<"type">> => ?TYPE_JOIN_READY,
        <<"data">> => #{
            <<"node_id">> => atom_to_binary(NodeId, utf8),
            <<"node_addr">> => list_to_binary(inet:ntoa(NodeAddr)),
            <<"node_port">> => NodePort,
            <<"hashes">> => [
                #{
                    <<"hash">> => Hash,
                    <<"node">> => atom_to_binary(Node, utf8)
                } || {Hash, Node} <- Hashes
            ]
        }
    }.

%%====================================================================
%% Private – map → struct
%%====================================================================

map_to_struct(#{<<"type">> := ?TYPE_GET,
                <<"data">> := Data}) ->
    #get{
        key = maps:get(<<"key">>, Data),
        ref = try_decode_ref(Data)
    };

map_to_struct(#{<<"type">> := ?TYPE_GET_RESP,
                <<"status">> := StBin,
                <<"data">> := Data}) ->
    #get_response{
        status = status_atom(StBin),
        key = maps:get(<<"key">>, Data),
        value = maps:get(<<"value">>, Data),
        ref = try_decode_ref(Data)
    };

map_to_struct(#{<<"type">> := ?TYPE_SET,
                <<"data">> := Data}) ->
    #set{
        key = maps:get(<<"key">>, Data),
        value = maps:get(<<"value">>, Data),
        ref = try_decode_ref(Data)
    };

map_to_struct(#{<<"type">> := ?TYPE_SET_RESP,
                <<"status">> := StBin,
                <<"data">> := Data}) ->
    #set_response{
        status = status_atom(StBin),
        key = maps:get(<<"key">>, Data),
        value = maps:get(<<"value">>, Data),
        ref = try_decode_ref(Data)
    };

map_to_struct(#{<<"type">> := ?TYPE_JOIN_INIT,
                <<"data">> := #{<<"node_id">> := NodeId, <<"node_addr">> := NodeAddr, <<"node_port">> := NodePort}}) ->
    #join_init{
        node_id = binary_to_atom(NodeId, utf8),
        address = element(2, inet:parse_address(binary_to_list(NodeAddr))),
        port = NodePort
    };

map_to_struct(#{<<"type">> := ?TYPE_JOIN_INIT_RESP,
                <<"data">> := #{<<"nodes">> := Nodes, <<"hashes">> := Hashes}}) ->
    #join_init_response{
        nodes = [
            {binary_to_atom(NodeId, utf8), element(2, inet:parse_address(binary_to_list(NodeAddr))), NodePort} ||
            #{<<"node_id">> := NodeId, <<"node_addr">> := NodeAddr, <<"node_port">> := NodePort} <- Nodes
        ],
        hashes = [
            {Hash, binary_to_atom(Node, utf8)} || 
            #{<<"hash">> := Hash, <<"node">> := Node} <- Hashes
        ]
    };

map_to_struct(#{<<"type">> := ?TYPE_JOIN_GET_KEYS,
                <<"data">> := #{<<"node_id">> := NodeId, <<"hashes">> := HashRanges}}) ->
    #join_get_keys{
        node_id = binary_to_atom(NodeId, utf8),
        hash_ranges = [
            {InitialHash, LastHash} || 
            #{<<"start">> := InitialHash, <<"end">> := LastHash} <- HashRanges
        ]
    };

map_to_struct(#{<<"type">> := ?TYPE_JOIN_GET_KEYS_RESP,
                <<"data">> := TransferData}) ->
    #join_get_keys_response{
        transfer_map = TransferData
    };

map_to_struct(#{<<"type">> := ?TYPE_JOIN_READY,
                <<"data">> := #{<<"node_id">> := NodeId, <<"node_addr">> := NodeAddr, <<"node_port">> := NodePort, <<"hashes">> := Hashes}}) ->
    #join_ready{
        node_id = binary_to_atom(NodeId, utf8),
        address = element(2, inet:parse_address(binary_to_list(NodeAddr))),
        port = NodePort,
        hashes = [
            {Hash, binary_to_atom(Node, utf8)} || 
            #{<<"hash">> := Hash, <<"node">> := Node} <- Hashes
        ]
    };

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

try_encode_ref(BaseData, Ref) ->
    case Ref of
        undefined -> 
            BaseData;
        _ ->
            Bin = term_to_binary(Ref),
            maps:put(<<"ref">>, base64:encode_to_string(Bin), BaseData)
    end.

try_decode_ref(Data) ->
    case maps:find(<<"ref">>, Data) of
        error -> undefined;
        {ok, Bin} -> binary_to_term(base64:decode(Bin))
    end.
