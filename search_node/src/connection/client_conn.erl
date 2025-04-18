-module(client_conn).

-export([start_link/1]).

start_link(Socket) ->
	Pid = spawn(fun() -> loop(Socket) end),
	{ok, Pid}.

loop(Socket) ->
    inet:setopts(Socket, [{active, once}]),
    receive
        {tcp, Socket, Data} ->
            process_message(Socket, Data),
            loop(Socket);  %% loop again

        {tcp_closed, Socket} ->
            io:format("Socket closed~n"),
            gen_tcp:close(Socket),
            ok;

        {tcp_error, Socket, Reason} ->
            io:format("Socket error: ~p~n", [Reason]),
            gen_tcp:close(Socket),
            ok;

        Other ->
            io:format("Unknown message: ~p~n", [Other]),
            loop(Socket)
    end.

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
            io:format("Stored key-value pair: ~p -> ~p~n", [Key, CleanValue]),
            gen_tcp:send(Socket, "ok\n");

        _ ->
            io:format("Invalid command: ~p~n", [Command]),
            gen_tcp:send(Socket, "Invalid command\n")
    end.

clean_data(Data) ->
    RawParts = string:tokens(binary_to_list(Data), " "),
    lists:map(fun string:trim/1, RawParts).

trim_newline(Str) ->
    case lists:reverse(Str) of
        [$\n | Rest] -> lists:reverse(Rest);
        _ -> Str
    end.

