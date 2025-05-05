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
    #join_disseminate{}) -> iodata().
encode(Struct) ->
    Map = struct_to_map(Struct),
	json:encode(Map).

-spec decode(binary() | iolist()) -> 
    #get{} | #get_response{} | 
    #set{} | #set_response{} | 
    #join_init{} | #join_init_response{} |
    #join_get_keys{} | #join_get_keys_response{} |
    #join_disseminate{} | no_return().
decode(Bin) ->
	Map = json:decode(iolist_to_binary(Bin)),
    map_to_struct(Map).

%%====================================================================
%% Private – struct → map
%%====================================================================

struct_to_map(#get{is_peer = IsPeer, key = Key}) ->
    #{
        <<"type">> => ?TYPE_GET,
        <<"is_peer">> => IsPeer,
        <<"data">> => #{<<"key">> => Key}
    };

struct_to_map(#get_response{status = Status, key = Key, value = Val}) ->
    #{
        <<"type">> => ?TYPE_GET_RESP,
        <<"status">> => status_bin(Status),
        <<"data">> => #{
            <<"key">> => Key,
            <<"value">> => Val
        }
    };

struct_to_map(#set{is_peer = IsPeer, key = Key, value = Val}) ->
    #{
        <<"type">> => ?TYPE_SET,
        <<"is_peer">> => IsPeer,
        <<"data">> => #{
            <<"key">> => Key,
            <<"value">> => Val
        }
    };

struct_to_map(#set_response{status = Status, key = Key, value = Val}) ->
    #{
        <<"type">> => ?TYPE_SET_RESP,
        <<"status">> => status_bin(Status),
        <<"data">> => #{
            <<"key">> => Key,
            <<"value">> => Val
        }
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

struct_to_map(#join_get_keys_response{keys = Keys, values = Values}) ->
    #{
        <<"type">> => ?TYPE_JOIN_GET_KEYS_RESP,
        <<"data">> => #{
            <<"keys">> => Keys,
            <<"values">> => Values
        }
    };

struct_to_map(#join_disseminate{node_id = NodeId, hashes = Hashes}) ->
    #{
        <<"type">> => ?TYPE_JOIN_DISSEMINATE,
        <<"data">> => #{
            <<"node_id">> => atom_to_binary(NodeId, utf8),
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
                <<"is_peer">> := IsPeer,
                <<"data">> := #{<<"key">> := Key}}) ->
    #get{
        is_peer = IsPeer, 
        key = Key
    };

map_to_struct(#{<<"type">> := ?TYPE_GET_RESP,
                <<"status">> := StBin,
                <<"data">> := #{<<"key">> := Key, <<"value">> := Val}}) ->
    #get_response{
        status = status_atom(StBin),
        key = Key,
        value = Val
    };

map_to_struct(#{<<"type">> := ?TYPE_SET,
                <<"is_peer">> := IsPeer,
                <<"data">> := #{<<"key">> := Key, <<"value">> := Val}}) ->
    #set{
        is_peer = IsPeer,
        key = Key,
        value = Val
    };

map_to_struct(#{<<"type">> := ?TYPE_SET_RESP,
                <<"status">> := StBin,
                <<"data">> := #{<<"key">> := Key, <<"value">> := Val}}) ->
    #set_response{
        status = status_atom(StBin),
        key = Key,
        value = Val
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
                <<"data">> := #{<<"keys">> := Keys, <<"values">> := Values}}) ->
    #join_get_keys_response{
        keys = Keys,
        values = Values
    };

map_to_struct(#{<<"type">> := ?TYPE_JOIN_DISSEMINATE,
                <<"data">> := #{<<"node_id">> := NodeId, <<"hashes">> := Hashes}}) ->
    #join_disseminate{
        node_id = binary_to_atom(NodeId, utf8),
        hashes = [
            {Hash, Node} || 
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
