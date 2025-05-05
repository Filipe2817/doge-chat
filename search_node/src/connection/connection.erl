-module(connection).
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

handle_command(Socket, #get{key = Key}) ->
    ReplyRec =
        case state_manager:get(Key) of
            not_found       -> proto:get_resp(not_found, Key, undefined);
            {error, _Rsn}   -> proto:get_resp(error,     Key, undefined);
            Value           -> proto:get_resp(ok,        Key, Value)
        end,
    gen_tcp:send(Socket, codec:encode(ReplyRec));

handle_command(Socket, #set{is_peer = _IsPeer, key = Key, value = Val}) ->
    ok      = state_manager:put(Key, Val),
    Reply   = proto:set_resp(ok, Key, Val),
    gen_tcp:send(Socket, codec:encode(Reply));

handle_command(Socket, #join_init{node_id = NodeId, address = NodeAddr, port = NodePort}) ->
    io:format("Join init from node ~p~n", [NodeId]),
    ok = state_manager:bind_process_node(NodeId, NodeAddr, NodePort, self()),
    {Nodes, Hashes} = state_manager:get_ring_state(),
    Reply = proto:join_init_resp(Nodes, Hashes),
    gen_tcp:send(Socket, codec:encode(Reply));

handle_command(_Socket, #join_init_response{nodes = Nodes, hashes = Hashes}) ->
    io:format("Join init response ~n"),
    ok = state_manager:update_ring_state(Nodes, Hashes),
    {MyId, MyHashes} = state_manager:get_node_info(),
    {_, UpdatedRing} = state_manager:get_ring_state(),
    Successors = dht:find_successors_range(MyHashes, UpdatedRing),
    state_manager:debug(),
    io:format("Successors: ~p~n", [Successors]),
    lists:foreach(
        fun({NodeId, Ranges}) ->
            case state_manager:get_node_connection(NodeId) of
                not_found ->
                    io:format("Node ~p not found in the ring~n", [NodeId]),
                    ok;
                Pid ->
                    Msg = proto:join_get_keys(MyId, Ranges),
                    connector:send_msg(Pid, Msg)
            end
        end,
        maps:to_list(Successors)
    );

handle_command(_Socket, #join_get_keys{node_id = NodeId, hash_ranges = _HashRanges}) ->
    io:format("Join get keys from node ~p~n", [NodeId]),
    ok;
%
%handle_command(Socket, #join_get_keys_response{keys = Keys, values = Values}) ->
%    io:format("Received join get keys response"),
%    ok;
%
%handle_command(Socket, #join_disseminate{node_id = NodeId, hashes = Hashes}) ->
%    io:format("Join disseminate from node ~p~n", [NodeId]),
%    ok;

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
