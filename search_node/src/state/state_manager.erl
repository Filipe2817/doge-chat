-module(state_manager).
-behaviour(gen_server).

%% API
-export([
    start_link/4, 
    get/1, 
    put/2, 
    bind_process_node/2, bind_process_node/4,
    join_init/1,
    get_ring_state/0,
    update_ring_state/2,
    get_node_info/0,
    get_node_connection/1,
    debug/0
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%%--------------------------------------------------------------------
%% API Functions
%%--------------------------------------------------------------------
start_link(NodeId, Addr, Port, KnownNodes) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [NodeId, Addr, Port, KnownNodes], []).

%% get(Key) returns the value or not_found if Key does not exist.
get(Key) ->
	gen_server:call(?MODULE, {get, Key}).

%% put(Key, Value) stores the value and returns ok.
put(Key, Value) ->
	gen_server:call(?MODULE, {put, Key, Value}).

bind_process_node(Node, Pid) ->
    gen_server:call(?MODULE, {bind_process_node, Node, #{pid => Pid}}).

bind_process_node(Node, Addr, Port, Pid) ->
    gen_server:call(?MODULE, {bind_process_node, Node, #{addr => Addr, port => Port, pid => Pid}}).

join_init(NodeToConn) ->
    gen_server:call(?MODULE, {join_init, NodeToConn}).

get_ring_state() ->
    gen_server:call(?MODULE, {get_ring_state}).

update_ring_state(Nodes, Hashes) ->
    gen_server:call(?MODULE, {update_ring_state, Nodes, Hashes}).

get_node_info() ->
    gen_server:call(?MODULE, {get_node_info}).

get_node_connection(NodeId) ->
    gen_server:call(?MODULE, {get_node_connection, NodeId}).

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

handle_call({bind_process_node, Node, Args}, _From, State) ->
    Endpoints = maps:get(endpoints, State),
    Current = case maps:find(Node, Endpoints) of
        {ok, {A, P, _}} ->
            #{addr => A, port => P};
        error ->
            #{addr => undefined, port => undefined}
    end,
    NewEndpoint = maps:merge(Current, Args),
    #{addr := Addr, port := Port, pid := Pid} = NewEndpoint,
    NewEndpoints = maps:put(Node, {Addr, Port, Pid}, Endpoints),
    NewState = maps:put(endpoints, NewEndpoints, State),
    {reply, ok, NewState};

handle_call({join_init, NodeToConn}, _From, State) ->
    Endpoints = maps:get(endpoints, State),
    Endpoint = maps:get(NodeToConn, Endpoints, not_found),
    NodeId = maps:get(node_id, State),
    SocketAddr = maps:get(socket_addr, State),
    {reply, {NodeId, SocketAddr, Endpoint}, State};

handle_call({get_ring_state}, _From, State) ->
    Endpoints = maps:get(endpoints, State),
    Nodes = maps:fold(
        fun(NodeId, {Addr, Port, _}, Acc) ->
            [{NodeId, Addr, Port} | Acc]
        end, 
        [], 
        Endpoints
    ),
    Hashes = maps:get(ring, State),
    {reply, {Nodes, Hashes}, State};

handle_call({update_ring_state, Nodes, Hashes}, _From, State) ->
    % Nodes Update
    MyId = maps:get(node_id, State),
    Endpoints = maps:get(endpoints, State),
    NodesMapped = maps:from_list([{NodeId, {Addr, Port, undefined}} || {NodeId, Addr, Port} <- Nodes, NodeId =/= MyId]),
    NewEndpoints = maps:merge(NodesMapped, Endpoints),
    NewState = maps:put(endpoints, NewEndpoints, State),
    % Ring Update
    CurrentRing = maps:get(ring, State),
    NewRing = lists:usort(CurrentRing ++ Hashes),
    FinalNewState = maps:put(ring, NewRing, NewState),
    {reply, ok, FinalNewState};

handle_call({get_node_info}, _From, State) ->
    NodeId = maps:get(node_id, State),
    MyHashes = maps:get(my_hashes, State),
    {reply, {NodeId, MyHashes}, State};

handle_call({get_node_connection, NodeId}, _From, State) ->
    Endpoints = maps:get(endpoints, State),
    {ResPid, ResState} = case maps:find(NodeId, Endpoints) of
        {ok, {Addr, Port, undefined}} ->
            Pid = connector:create_connection(NodeId, Addr, Port),
            NewEndpoints = maps:put(NodeId, {Addr, Port, Pid}, Endpoints),
            NewState = maps:put(endpoints, NewEndpoints, State),
            {Pid, NewState};
        {ok, {_Addr, _Port, Pid}} when is_pid(Pid) ->
            {Pid, State};
        error ->
            {not_found, State}
    end,
    {reply , ResPid, ResState};

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
