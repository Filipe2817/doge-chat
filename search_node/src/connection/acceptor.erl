-module(acceptor).
-behaviour(gen_server).

% API
-export([start_link/1]).

% GenServer callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
		 terminate/2]).

%% Struct to hold the State
-record(state, {
	listen_socket,
	port
}).

%%--------------------------------------------------------------------
%% API Functions
%%--------------------------------------------------------------------
start_link(Port) ->
	io:format("Starting acceptor on port ~p~n", [Port]),
	gen_server:start_link({local, ?MODULE}, ?MODULE, Port, []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
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
	{ok, #state{
		listen_socket = ListenSocket,
		port = Port
	}}.

handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast(_Msg, State) ->
	{ok, State}.

handle_info(accept, State = #state{listen_socket = ListenSocket}) ->
	case gen_tcp:accept(ListenSocket) of
		{ok, Socket} ->
			%% Wait for the init message
			{ok, BinLine} = gen_tcp:recv(Socket, 0),
			JsonMap = json:decode(BinLine),
			handle_request(Socket, JsonMap),
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
