-module(state_manager).
-behaviour(gen_server).

%% API
-export([
    start_link/4,
    % Store Interaction
    get/1,
    put/2,
    % Endpoints
    add_endpoint/3,
    get_endpoint_connection/1,
    get_endpoints/0,
    update_endpoints/1,
    % Ring
    get_ring/0,
    update_ring/1,
    % Transfer Keys
    transfer_keys/1,
    update_key_map/1,
    % Utils
    get_node_info/0,
    debug/0
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%%--------------------------------------------------------------------
%% API Functions
%%--------------------------------------------------------------------
start_link(NodeId, Addr, Port, KnownNodes) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [NodeId, Addr, Port, KnownNodes], []).

%% Store Interaction

get(Key) ->
	gen_server:call(?MODULE, {get, Key}).

put(Key, Value) ->
	gen_server:call(?MODULE, {put, Key, Value}).

%% Endpoints

add_endpoint(NodeId, Addr, Port) ->
    gen_server:call(?MODULE, {add_endpoint, NodeId, Addr, Port}).

get_endpoint_connection(NodeId) ->
    gen_server:call(?MODULE, {get_endpoint_connection, NodeId}).

get_endpoints() ->
    gen_server:call(?MODULE, {get_endpoints}).

update_endpoints(Nodes) ->
    gen_server:call(?MODULE, {update_endpoints, Nodes}).

%% Ring

get_ring() ->
    gen_server:call(?MODULE, {get_ring}).

update_ring(Hashes) ->
    gen_server:call(?MODULE, {update_ring, Hashes}).

%% Transfer Keys

transfer_keys(Ranges) ->
    gen_server:call(?MODULE, {transfer_keys, Ranges}).

update_key_map(NewMap) ->
    gen_server:call(?MODULE, {update_key_map, NewMap}).

%% Utils

get_node_info() ->
    gen_server:call(?MODULE, {get_node_info}).

debug() ->
    gen_server:call(?MODULE, {debug}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
init([NodeId, Addr, Port, KnownNodes]) ->
    NodeHashes = dht:gen_virtual_nodes_hashes(NodeId),
    MyHashes = lists:sort([{Hash, NodeId} || Hash <- NodeHashes]),
    State = #{
        node_id     => NodeId,
        socket_addr => {Addr, Port},
        topics      => #{},                                 % string => [ {Ip, Port} ]
        my_hashes   => MyHashes,                            % [ {NodeHash, NodeId} ]
        ring        => MyHashes,                            % [ {NodeHash, NodeId} ]
        endpoints   => extract_known_nodes(#{}, KnownNodes) % NodeId => {Ip, Port, Pid}
    },
    {ok, State}.


%%%%%%%%%%%%%%% Store Interaction

handle_call({get, Key}, _From, State) ->
    TopicsMap = maps:get(topics, State),
    Value = maps:get(Key, TopicsMap, not_found),
    {reply, Value, State};

% Node = get_responsible_node(Key, State),
% MyId = maps:get(node_id, State),
% if Node =:= MyId ->
%    execute_read
%    reply (client / server)
% true ->
%    redirect

handle_call({put, Key, Value}, _From, State) ->
    TopicsMap = maps:get(topics, State),
    CurrentValue = maps:get(Key, TopicsMap, []),
    NewValue = [Value | CurrentValue],
    NewTopicsMap = maps:put(Key, NewValue, TopicsMap),
    NewState = maps:put(topics, NewTopicsMap, State),
    {reply, ok, NewState};


% Node = get_responsible_node(Key, State),
% MyId = maps:get(node_id, State),
% if Node =:= MyId ->
%    execute_write
%    reply (client / server)
% true ->
%    redirect

%%%%%%%%%%%%%%% Endpoints

handle_call({add_endpoint, NodeId, Addr, Port}, From, State) ->
    Endpoints = maps:get(endpoints, State),
    NewEndpoints = maps:put(NodeId, {Addr, Port, From}, Endpoints),
    NewState = maps:put(endpoints, NewEndpoints, State),
    {reply, ok, NewState};

handle_call({get_endpoint_connection, NodeId}, _From, State) ->
    Endpoints = maps:get(endpoints, State),
    case maps:get(NodeId, Endpoints, not_found) of
        not_found ->
            {reply, not_found, State};
        {Addr, Port, undefined} ->
            Pid = open_connection(Addr, Port),
            NewEndpoints = maps:put(NodeId, {Addr, Port, Pid}, Endpoints),
            NewState = maps:put(endpoints, NewEndpoints, State),
            {reply, Pid, NewState};
        {_Addr, _Port, Pid} ->
            {reply, Pid, State}
    end;

handle_call({get_endpoints}, _From, State) ->
    Endpoints = maps:get(endpoints, State),
    Nodes = maps:fold(
        fun(NodeId, {Addr, Port, _}, Acc) ->
            [{NodeId, Addr, Port} | Acc]
        end,
        [],
        Endpoints
    ),
    {reply, Nodes, State};

handle_call({update_endpoints, Nodes}, _From, State) ->
    MyId = maps:get(node_id, State),
    Endpoints = maps:get(endpoints, State),
    NewEndpoints = lists:foldl(
        fun({NodeId, Addr, Port}, Acc) ->
            case maps:get(NodeId, Acc, not_found) of
                not_found -> % open connection
                    Pid = open_connection(Addr, Port),
                    maps:put(NodeId, {Addr, Port, Pid}, Acc);
                {Addr, Port, undefined} -> % open connection
                    Pid = open_connection(Addr, Port),
                    maps:put(NodeId, {Addr, Port, Pid}, Acc);
                {_Addr, _Port, _Pid} -> % nothing happens
                    Acc
            end
        end,
        Endpoints,
        lists:keydelete(MyId, 1, Nodes)
    ),
    NewState = maps:put(endpoints, NewEndpoints, State),
    {reply, ok, NewState};

%%%%%%%%%%%%%%% Ring

handle_call({get_ring}, _From, State) ->
    {reply, maps:get(ring, State), State};

handle_call({update_ring, Hashes}, _From, State) ->
    CurrentRing = maps:get(ring, State),
    NewRing = lists:usort(CurrentRing ++ Hashes),
    FinalNewState = maps:put(ring, NewRing, State),
    {reply, ok, FinalNewState};

%%%%%%%%%%%%%%%% Transfer Keys

handle_call({transfer_keys, Ranges}, _From, State) ->
    Map = maps:get(topics, State),
    {ToKeep, ToTransfer} = dht:divide_map_to_transfer(Map, Ranges),
    NewState = maps:put(topics, ToKeep, State),
    {reply , ToTransfer, NewState};

handle_call({update_key_map, NewMap}, _From, State) ->
    TopicsMap = maps:get(topics, State),
    NewTopicsMap = maps:merge(TopicsMap, NewMap),
    NewState = maps:put(topics, NewTopicsMap, State),
    {reply, ok, NewState};

%%%%%%%%%%%%%%% Utils

handle_call({get_node_info}, _From, State) ->
    NodeId = maps:get(node_id, State),
    SocketAddr = maps:get(socket_addr, State),
    MyHashes = maps:get(my_hashes, State),
    {reply, {NodeId, SocketAddr, MyHashes}, State};

handle_call({debug}, _From, State) ->
    io:format("State: ~p~n", [State]),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, error, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

extract_known_nodes(Map, []) ->
    Map;

extract_known_nodes(Map, [{NodeId, Addr, Port} | Rest]) ->
    NewMap = maps:put(NodeId, {Addr, Port, undefined}, Map),
    extract_known_nodes(NewMap, Rest).

open_connection(Addr, Port) ->
    {ok, Socket} = gen_tcp:connect(Addr, Port, [binary, {active, once}]),
    {ok, Pid} = connection_sup:start_child(Socket),
    ok = gen_tcp:controlling_process(Socket, Pid),
    Pid.
