-module(acceptor).

% API
-export([start_link/2]).

%% Struct to hold the State
%-record(state, {
%	listen_socket,
%	port
%}).

start_link(Addr, Port) ->
	io:format("Starting acceptor on ~p:~p~n", [Addr, Port]),
	Options = [
		binary,
		{active, false},
		{packet, 0},
		{reuseaddr, true},
		{backlog, 5},
		{ip, Addr}
	],
	{ok, ListenSocket} = gen_tcp:listen(Port, Options),
	ok = request_join(n1),
	Pid = spawn(fun() -> loop(ListenSocket) end),
	{ok, Pid}.

loop(LSocket) ->
	case gen_tcp:accept(LSocket) of
		{ok, Socket} ->
			{ok, Pid} = connection_sup:start_child(Socket),
			ok = gen_tcp:controlling_process(Socket, Pid),
			loop(LSocket);
		{error, Reason} ->
			io:format("Error accepting connection: ~p~n", [Reason]),
			loop(LSocket)
	end.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

request_join(Node) ->
	{MyId, {MyAddr, MyPort}, _} = state_manager:get_node_info(),
	case state_manager:get_endpoint_connection(Node) of
		not_found ->
			io:format("Node ~p not found~n", [Node]),
			ok;
		Pid ->
			connection:send_msg(Pid, proto:join_init(MyId, MyAddr, MyPort))
	end.
