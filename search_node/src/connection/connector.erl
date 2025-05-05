-module(connector).

-export([create_connection/3, send_msg/2, request_join/1]).

create_connection(Node, Addr, Port) ->
    {ok, Socket} = gen_tcp:connect(Addr, Port, [binary, {active, once}]),
    {ok, Pid} = connection_sup:start_child(Socket),
    ok = gen_tcp:controlling_process(Socket, Pid),
    ok = state_manager:bind_process_node(Node, Pid),
    io:format("Connected to node ~p~n", [Node]),
    Pid.

send_msg(Pid, Msg) ->
    Pid ! {send, Msg},
    ok.

request_join(Node) ->
    {MyId, {MyAddr, MyPort}, Endpoint} = state_manager:join_init(Node),
    case Endpoint of
        not_found ->
            io:format("Node ~p not found in the ring~n", [Node]),
            ok;
        {Addr, Port, undefined} ->
            Pid = create_connection(Node, Addr, Port),
            send_msg(Pid, proto:join_init(MyId, MyAddr, MyPort))
    end.
