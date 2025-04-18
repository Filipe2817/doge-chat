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
   {ok, Socket}.

handle_call(_Msg, _From, Socket) ->
    {reply, ok, Socket}.

handle_cast(_Msg, Socket) ->
    {noreply, Socket}.

handle_info({tcp, Socket, Data}, State) ->
	process_message(Socket, Data),
    {noreply, State};
handle_info({tcp_closed, _Socket}, State) ->
    io:format("Socket closed~n"),
    {stop, normal, State};
handle_info(Info, State) ->
	io:format("Unhandled info: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, Socket) ->
	io:format("Terminating connection: ~p~n", [Socket]),
    gen_tcp:close(Socket),
    ok.

process_message(Socket, Data) ->
	Command = clean_data(Data),
    case Command of
        ["GET", Key] ->
            io:format("Handling GET request for key: ~p~n", [Key]),
            Value = state_manager:get(Key),
            Reply = case Value of
                        not_found -> "not_found\n";
                        _ -> Value ++ "\n"
                    end,
            gen_tcp:send(Socket, Reply);
        ["PUT", Key, Value] ->
            CleanValue = trim_newline(Value),
            state_manager:put(Key, CleanValue),
            io:format("Stored key-value pair: ~p -> ~p~n", [Key, CleanValue]);
        _ ->
            io:format("Invalid command: ~p~n", [Data]),
			gen_tcp:send(Socket, "Invalid command\n")
    end.

clean_data(Data) ->
	RawParts = string:tokens(binary_to_list(Data), " "),
    lists:map(fun(X) -> string:trim(X) end, RawParts).

trim_newline(Str) ->
    case lists:reverse(Str) of
        [$\n | Rest] -> lists:reverse(Rest);
        _ -> Str
    end.

