-module(acceptor).
-behaviour(gen_server).

% API
-export([start_link/1]).

% GenServer callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
		 terminate/2]).

% GenServer client API
start_link(Port) ->
	io:format("Starting acceptor on port ~p~n", [Port]),
	gen_server:start_link({local, ?MODULE}, ?MODULE, Port, []).

%% GenServer callbacks
init(Port) ->
	Options = [
		binary,
		{active, false},
		{packet, 0},
		{reuseaddr, true},
		{backlog, 5}
	],

	{ok, ListenSocket} = gen_tcp:listen(Port, Options),
	self() ! accept,
	{ok, {ListenSocket, Port}}.

handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast(_Msg, State) ->
	{ok, State}.

handle_info(accept, {ListenSocket, _Port} = State) ->
	case gen_tcp:accept(ListenSocket) of
		{ok, Socket} ->
			io:format("Accepted connection: ~p~n", [Socket]),
			{ok, Pid} = client_conn:start_link(Socket),
			ok = gen_tcp:controlling_process(Socket, Pid),
			self() ! accept,
			{noreply, State};
		{error, Reason} ->
			io:format("Error accepting connection: ~p~n", [Reason]),
			{stop, Reason, State}
	end;

handle_info(_Info, State) ->
	%% Handle other messages if needed
	{noreply, State}.

terminate(_Reason, _State) ->
	%% Clean up resources if needed
	ok.
