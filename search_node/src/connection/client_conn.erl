%% In client_conn.erl (implementing your connection handler)
-module(client_conn).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%%--------------------------------------------------------------------
%% API Functions
%%--------------------------------------------------------------------
start_link(Socket) ->
    gen_server:start_link(?MODULE, Socket, []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
init(Socket) ->
	io:format("Initializing client connection: ~p~n", [Socket]),
	ok = inet:setopts(Socket, [{active, true}]),
    {ok, Socket}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Data}, Socket) ->
    io:format("Received data: ~p~n", [Data]),
    {noreply, Socket};
handle_info({tcp_closed, Socket}, Socket) ->
    io:format("Socket closed: ~p~n", [Socket]),
    {stop, normal, Socket};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, Socket) ->
	io:format("Terminating connection: ~p~n", [Socket]),
    gen_tcp:close(Socket),
    ok.
