-module(acceptor).
-behaviour(gen_server).

% API
-export([start_link/2]).

% GenServer callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
		 terminate/2]).

%% Struct to hold the State
-record(state, {
	listen_socket,
	port,
	sm_pid
}).

%%--------------------------------------------------------------------
%% API Functions
%%--------------------------------------------------------------------
start_link(Port, SM) ->
	io:format("Starting acceptor on port ~p~n", [Port]),
	gen_server:start_link({local, ?MODULE}, ?MODULE, {Port, SM}, []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
init({Port, SM}) ->
    %% Retrieve the actual pid (in case SM was passed as the registered name)
    SM_Pid = case is_pid(SM) of
                true -> SM;
                false -> whereis(SM)
            end,

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
		port = Port,
		sm_pid = SM_Pid
	}}.

handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast(_Msg, State) ->
	{ok, State}.

handle_info(accept, State = #state{listen_socket = ListenSocket}) ->
	case gen_tcp:accept(ListenSocket) of
		{ok, Socket} ->
			io:format("Accepted connection: ~p~n", [Socket]),
			{ok, Pid} = connection_sup:start_child(Socket),
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
