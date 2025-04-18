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
	%% Accept incoming connections
	LS = State#state.listen_socket,
	case gen_tcp:accept(LS) of
		{ok, Socket} ->
			%% Wait for the init message
			{ok, BinLine} = gen_tcp:recv(Socket, 0),
			JsonMap = json:decode(BinLine),
			handle_request(Socket, JsonMap),
			loop(State);
		{error, Reason} ->
			io:format("Error accepting connection: ~p~n", [Reason]),
			loop(State)
	end.

handle_request(Socket, JsonMap) ->
	case maps:get(<<"type">>, JsonMap) of
		<<"init">> ->
			case maps:get(<<"client_type">>, JsonMap) of
               <<"client">> ->
                   io:format("Handling search node connection~n"),
                   {ok, Pid} = connection_sup:start_child(Socket),
                   ok = gen_tcp:controlling_process(Socket, Pid),
                   ok = inet:setopts(Socket, [{active, true}]);
               <<"peer">> ->
                   io:format("Handling search client connection~n"),
                   {ok, Pid} = connection_sup:start_child(Socket),
                   ok = gen_tcp:controlling_process(Socket, Pid),
                   ok = inet:setopts(Socket, [{active, true}]);
				_ ->
					io:format("Unknown client type~n")
			end;
		_ ->
			io:format("Unknown connection type~n")
	end.
