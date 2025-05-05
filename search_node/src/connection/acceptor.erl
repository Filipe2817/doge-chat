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
	ok = connector:request_join(n1),
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
