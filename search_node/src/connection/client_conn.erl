-module(client_conn).
-include("../protocol/proto.hrl").

-export([start_link/1]).

start_link(Socket) ->
    io:format("New connection [~p:~p]~n", [self(), Socket]),
	ok = inet:setopts(Socket, [{packet, 4}, {active, once}]),
	Pid = spawn(fun() -> loop(Socket) end),
	{ok, Pid}.

loop(Socket) ->
    receive
        {tcp, Socket, WireBin} ->
            ok = inet:setopts(Socket, [{active, once}]),
            case safe_decode(WireBin) of
                {ok, MsgRec} ->
                    handle_command(Socket, MsgRec),
                    loop(Socket);
                {error, Reason} ->
                    error_logger:error_msg("Bad packet: ~p~n", [Reason]),
                    gen_tcp:send(Socket, 
                        codec:encode(
                            proto:get_resp(error, <<>>, <<>>)
                        )
                    ),
                    loop(Socket)
            end;

        {tcp_closed, Socket} ->
            io:format("Socket closed~n"),
            gen_tcp:close(Socket),
            ok;

        {tcp_error, Socket, Reason} ->
            io:format("Socket error: ~p~n", [Reason]),
            gen_tcp:close(Socket),
            ok;

        %% Any other message sent to this process
        Other ->
            io:format("Unknown message: ~p~n", [Other]),
            loop(Socket)
    end.

%%--------------------------------------------------------------------
%% Business logic – record based
%%--------------------------------------------------------------------

handle_command(Socket, #get{key = Key}) ->
    ReplyRec =
        case state_manager:get(Key) of
            not_found       -> proto:get_resp(not_found, Key, undefined);
            {error, _Rsn}   -> proto:get_resp(error,     Key, undefined);
            Value           -> proto:get_resp(ok,        Key, Value)
        end,
    gen_tcp:send(Socket, codec:encode(ReplyRec));

handle_command(Socket, #set{is_peer = _IsPeer,
                            key = Key,
                            value = Val}) ->
    ok      = state_manager:put(Key, Val),
    Reply   = proto:set_resp(ok, Key, Val),
    gen_tcp:send(Socket, codec:encode(Reply));

handle_command(Socket, UnknownRec) ->
    io:format("Unknown packet: ~p~n", [UnknownRec]),
    ErrorRec = proto:get_resp(error, <<>>, <<>>),
    gen_tcp:send(Socket, codec:encode(ErrorRec)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

safe_decode(Bin) ->
    try
        {ok, codec:decode(Bin)}
    catch
        _:Err -> {error, Err}
    end.
