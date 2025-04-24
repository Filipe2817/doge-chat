-module(acceptor).

% API
-export([start_link/1]).

%% Struct to hold the State
-record(state, {
	listen_socket,
	port
}).

start_link(Port) ->
	io:format("Starting acceptor on port ~p~n", [Port]),
	Options = [
		binary,
		{active, false},
		{packet, 0},
		{reuseaddr, true},
		{backlog, 5}
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
