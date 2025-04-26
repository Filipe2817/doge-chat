-module(acceptor).

% API
-export([start_link/4]).

%% Struct to hold the State
-record(state, {
	listen_socket,
	port
}).

start_link(Addr, Port, _NodeName, _OtherNodes) ->
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
	State = #state{
		listen_socket = ListenSocket,
		port = Port
	},

	Pid = spawn(fun() -> loop(State) end),
	{ok, Pid}.

loop(State) ->
	LS = State#state.listen_socket,
	case gen_tcp:accept(LS) of
		{ok, Socket} ->
		    {ok, Pid} = connection_sup:start_child(Socket),
		    ok = gen_tcp:controlling_process(Socket, Pid),
			loop(State);
		{error, Reason} ->
			io:format("Error accepting connection: ~p~n", [Reason]),
			loop(State)
	end.
