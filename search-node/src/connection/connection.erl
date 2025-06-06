-module(connection).
-include("../protocol/proto.hrl").

-export([start_link/1, send_msg/2]).

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

        {send, Msg} ->
            gen_tcp:send(Socket, codec:encode(Msg)),
            loop(Socket);

        %% Any other message sent to this process
        Other ->
            io:format("Unknown message: ~p~n", [Other]),
            loop(Socket)
    end.

%%--------------------------------------------------------------------
%% Business logic – record based
%%--------------------------------------------------------------------

handle_command(Socket, #get{key = Key, ref = undefined}) -> % no reference -> handle or redirect
    Result = state_manager:get(Key),
    case Result of
        {redirect, Pid, Ref} ->
            Msg = proto:get(Key, Ref),
            send_msg(Pid, Msg);
        _ ->
            ReplyRec = case Result of
                not_found       -> proto:get_resp(not_found, Key, undefined);
                {error, _Rsn}   -> proto:get_resp(error,     Key, undefined);
                Value           -> proto:get_resp(ok,        Key, Value)
            end,
            gen_tcp:send(Socket, codec:encode(ReplyRec))
    end;

handle_command(Socket, #get{key = Key, ref = Ref}) -> % reference -> handle
    ReplyRec =
        case state_manager:get(Key) of
            not_found       -> proto:get_resp(not_found, Key, undefined, Ref);
            {error, _Rsn}   -> proto:get_resp(error,     Key, undefined, Ref);
            Value           -> proto:get_resp(ok,        Key, Value, Ref)
        end,
    gen_tcp:send(Socket, codec:encode(ReplyRec));

handle_command(_Socket, #get_response{status = Status, key = Key, value = Val, ref = Ref}) -> % send to client
    Pid = state_manager:get_pending_pid(Ref),
    Msg = proto:get_resp(Status, Key, Val),
    send_msg(Pid, Msg);

%%%%%%%%%%%%%%%%%%%%%%%%

handle_command(Socket, #set{key = Key, value = Val, ref = undefined}) -> % no reference -> handle or redirect
    case state_manager:put(Key, Val) of
        {redirect, Pid, Ref} ->
            Msg = proto:set(Key, Val, Ref),
            send_msg(Pid, Msg);
        ok ->
            Reply = proto:set_resp(ok, Key, Val),
            state_manager:debug(),
            gen_tcp:send(Socket, codec:encode(Reply))
    end;

handle_command(Socket, #set{key = Key, value = Val, ref = Ref}) -> % reference -> handle
    ok = state_manager:put(Key, Val),
    Reply = proto:set_resp(ok, Key, Val, Ref),
    state_manager:debug(),
    gen_tcp:send(Socket, codec:encode(Reply));

handle_command(_Socket, #set_response{status = Status, key = Key, value = Val, ref = Ref}) -> % send to client
    Pid = state_manager:get_pending_pid(Ref),
    Msg = proto:set_resp(Status, Key, Val),
    send_msg(Pid, Msg);

%%%%%%%%%%%%%%%%%%%%%%%%

handle_command(Socket, #join_init{node_id = NodeId, address = NodeAddr, port = NodePort}) ->
    io:format("Join init from node ~p~n", [NodeId]),
    ok = state_manager:add_endpoint(NodeId, NodeAddr, NodePort),
    Reply = proto:join_init_resp(
        state_manager:get_endpoints(),
        state_manager:get_ring()
    ),
    gen_tcp:send(Socket, codec:encode(Reply));

handle_command(_Socket, #join_init_response{nodes = Nodes, hashes = Hashes}) ->
    io:format("Join init response ~n"),
    ok = state_manager:update_endpoints(Nodes),
    ok = state_manager:update_ring(Hashes),
    {MyId, _, MyHashes} = state_manager:get_node_info(),
    UpdatedRing = state_manager:get_ring(),
    Successors = dht:find_successors_range(MyHashes, UpdatedRing),
    lists:foreach(
        fun({NodeId, Ranges}) ->
            case state_manager:get_endpoint_connection(NodeId) of
                not_found ->
                    io:format("Node ~p not found~n", [NodeId]),
                    ok;
                Pid ->
                    Msg = proto:join_get_keys(MyId, Ranges),
                    send_msg(Pid, Msg)
            end
        end,
        maps:to_list(Successors)
    );

handle_command(Socket, #join_get_keys{node_id = NodeId, hash_ranges = HashRanges}) ->
    io:format("Join get keys from node ~p~n", [NodeId]),
    TransferMap = state_manager:transfer_keys(HashRanges),
    Reply = proto:join_get_keys_resp(TransferMap),
    gen_tcp:send(Socket, codec:encode(Reply));

handle_command(_Socket, #join_get_keys_response{transfer_map = TransferMap}) ->
    io:format("Received join get keys response ~p~n", [TransferMap]),
    ok = state_manager:update_key_map(TransferMap),
    Nodes = state_manager:get_endpoints(),
    {MyId, {MyAddr, MyPort}, MyHashes} = state_manager:get_node_info(),
    lists:foreach(
        fun({NodeId, _, _}) ->
            case state_manager:get_endpoint_connection(NodeId) of
                not_found ->
                    io:format("Node ~p not found~n", [NodeId]),
                    ok;
                Pid ->
                    Msg = proto:join_ready(MyId, MyAddr, MyPort, MyHashes),
                    send_msg(Pid, Msg)
            end
        end,
        Nodes
    ),
    state_manager:debug();

handle_command(_Socket, #join_ready{node_id = NodeId, address = NodeAddr, port = NodePort, hashes = Hashes}) ->
    io:format("Join ready from node ~p~n", [NodeId]),
    ok = state_manager:add_endpoint(NodeId, NodeAddr, NodePort),
    ok = state_manager:update_ring(Hashes),
    state_manager:debug();

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

send_msg(Pid, Msg) ->
    Pid ! {send, Msg},
    ok.
