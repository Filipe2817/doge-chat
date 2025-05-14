%%% helpers for maelstrom %%%

send_msg(Dest, Body, State) ->
    MsgId = maps:get(msg_id, State) + 1,
    FullBody = maps:put(<<"msg_id">>, MsgId, Body),
    OutMsg = #{
        <<"src">> => maps:get(node_id, State),
        <<"dest">> => Dest,
        <<"body">> => FullBody
    },
    OutJson = iolist_to_binary(json:encode(OutMsg)),
    io:format("~s~n", [OutJson]),
    State#{msg_id := MsgId}.

reply(Msg, ReplyType, ExtraFields, State) ->
    Body = maps:get(<<"body">>, Msg),
    InReplyTo = maps:get(<<"msg_id">>, Body),
    ReplyBody = maps:merge(#{<<"type">> => ReplyType, <<"in_reply_to">> => InReplyTo}, ExtraFields),
    send_msg(maps:get(<<"src">>, Msg), ReplyBody, State).

broadcast(Type, ExtraFields, State) ->
    NodeId = maps:get(node_id, State),
    NodeIds = maps:get(node_ids, State, []),
    lists:foreach(
        fun(Dest) ->
            if Dest =/= NodeId ->
                send_msg(Dest, maps:merge(#{<<"type">> => Type}, ExtraFields), State);
            true ->
                ok
            end
        end,
        NodeIds
    ).

print(Format, Args) ->
    io:format(standard_error, Format, Args).
