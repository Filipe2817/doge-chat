#!/usr/bin/env escript

-module(lin_kv).
-export([main/1]).
% escript does not support multiple modules
% the only solution is to include header files with the functions
% having unused functions may cause warnings that break maelstrom execution
-include("helpers.hrl").
-include("dht.hrl").

main(_Args) ->
    loop(#{
        node_id => undefined,
        node_ids => [],
        msg_id => 0,
        store => #{},
        neighbors => [],
        node_hashes => []
    }).

loop(State) ->
    case io:get_line("") of
        eof ->
            print("EOF reached, exiting...~n", []),
            ok;
        {error, Reason} ->
            print("Error reading line: ~p~n", [Reason]),
            loop(State);
        Line ->
            print("Received line: ~s~n", [Line]),
            case json:decode(list_to_binary(Line)) of
                {error, Reason} ->
                    print("Failed to decode JSON: ~p~n", [Reason]),
                    loop(State);
                Msg ->
                    NewState = handle_message(Msg, State),
                    loop(NewState)
            end
    end.

handle_message(#{<<"body">> := Body} = Msg, State) ->
    Type = maps:get(<<"type">>, Body, <<>>),
    case Type of
        <<"init">> -> handle_init(Msg, State);
        <<"node_info">> -> handle_node_info(Msg, State);
        <<"forward_result">> -> handle_forward_result(Msg, State);
        <<"read">> -> handle_read(Msg, State);
        <<"redirected_read">> -> handle_redirected_read(Msg, State);
        <<"write">> -> handle_write(Msg, State);
        <<"redirected_write">> -> handle_redirected_write(Msg, State);
        <<"cas">> -> handle_cas(Msg, State);
        <<"redirected_cas">> -> handle_redirected_cas(Msg, State);
        _Other ->
            print("Unknown message type: ~p~n", [Type]),
            State
    end.

handle_init(Msg, State) ->
    Body = maps:get(<<"body">>, Msg),
    NodeId = maps:get(<<"node_id">>, Body),
    NodeIds = maps:get(<<"node_ids">>, Body, []),
    NodeHash = sha1(NodeId),
    NewState = State#{
        node_id := NodeId,
        node_ids := NodeIds,
        node_hashes := [{NodeHash, NodeId}]
    },
    FinalState = reply(Msg, <<"init_ok">>, #{}, NewState),
    broadcast(<<"node_info">>, #{<<"node_hash">> => NodeHash}, FinalState),
    FinalState.

handle_node_info(Msg, State) ->
    Body = maps:get(<<"body">>, Msg),
    Sender = maps:get(<<"src">>, Msg),
    NodeHash = maps:get(<<"node_hash">>, Body),
    #{node_hashes := NodeHashes} = State,
    NewNodeHashes = [{NodeHash, Sender} | NodeHashes],
    State#{node_hashes := lists:sort(NewNodeHashes)}.

handle_forward_result(Msg, State) ->
    Body = maps:get(<<"body">>, Msg),
    Type = maps:get(<<"original_type">>, Body),
    Client = maps:get(<<"client">>, Body),
    ClientMsgId = maps:get(<<"client_msg_id">>, Body),
    DefaultResponseBody = #{
        <<"type">> => Type,
        <<"in_reply_to">> => ClientMsgId
    },
    ResponseBody = case Type of
        <<"read_ok">> ->
            Value = maps:get(<<"value">>, Body),
            maps:put(<<"value">>, Value, DefaultResponseBody);
        <<"error">> ->
            ErrorCode = maps:get(<<"code">>, Body),
            maps:put(<<"code">>, ErrorCode, DefaultResponseBody);
        _ ->
            DefaultResponseBody
    end,
    send_msg(Client, ResponseBody, State).

%%%%%%%%%%%%%%%%%%%%%%%%%%%% READ %%%%%%%%%%%%%%%%%%%%%%%%%%%%%

execute_read(Key, State) ->
    #{store := Store} = State,
    Value = maps:get(Key, Store, null),
    {<<"read_ok">>, #{<<"value">> => Value}, State}.

handle_read(Msg, State) ->
    Body = maps:get(<<"body">>, Msg),
    Key = maps:get(<<"key">>, Body),
    Node = get_responsible_node(Key, State),
    MyId = maps:get(node_id, State),
    if Node =:= MyId ->
        {ResultType, ResultBody, NewState} = execute_read(Key, State),
        reply(Msg, ResultType, ResultBody, NewState);
    true ->
        NewBody = #{
            <<"type">> => <<"redirected_read">>,
            <<"key">> => Key,
            <<"client">> => maps:get(<<"src">>, Msg),
            <<"client_msg_id">> => maps:get(<<"msg_id">>, Body)
        },
        send_msg(Node, NewBody, State)
    end.

handle_redirected_read(Msg, State) ->
    Body = maps:get(<<"body">>, Msg),
    Key = maps:get(<<"key">>, Body),
    Client = maps:get(<<"client">>, Body),
    ClientMsgId = maps:get(<<"client_msg_id">>, Body),
    {ResultType, ResultBody, NewState} = execute_read(Key, State),
    NewBody = maps:merge(ResultBody, #{
        <<"original_type">> => ResultType,
        <<"client">> => Client, 
        <<"client_msg_id">> => ClientMsgId
    }),
    reply(Msg, <<"forward_result">>, NewBody, NewState).

%%%%%%%%%%%%%%%%%%%%%%%%%%%% WRITE %%%%%%%%%%%%%%%%%%%%%%%%%%%%

execute_write(Key, Value, State) ->
    #{store := Store} = State,
    NewState = State#{store := maps:put(Key, Value, Store)},
    {<<"write_ok">>, #{}, NewState}.

handle_write(Msg, State) ->
    Body = maps:get(<<"body">>, Msg),
    Key = maps:get(<<"key">>, Body, undefined),
    Node = get_responsible_node(Key, State),
    MyId = maps:get(node_id, State),
    if Node =:= MyId ->
        Value = maps:get(<<"value">>, Body),
        {ResultType, ResultBody, NewState} = execute_write(Key, Value, State),
        reply(Msg, ResultType, ResultBody, NewState);
    true ->
        NewBody = #{
            <<"type">> => <<"redirected_write">>,
            <<"key">> => Key,
            <<"value">> => maps:get(<<"value">>, Body),
            <<"client">> => maps:get(<<"src">>, Msg),
            <<"client_msg_id">> => maps:get(<<"msg_id">>, Body)
        },
        send_msg(Node, NewBody, State)
    end.

handle_redirected_write(Msg, State) ->
    Body = maps:get(<<"body">>, Msg),
    Key = maps:get(<<"key">>, Body),
    Value = maps:get(<<"value">>, Body),
    Client = maps:get(<<"client">>, Body),
    ClientMsgId = maps:get(<<"client_msg_id">>, Body),
    {ResultType, ResultBody, NewState} = execute_write(Key, Value, State),
    NewBody = maps:merge(ResultBody, #{
        <<"original_type">> => ResultType,
        <<"client">> => Client, 
        <<"client_msg_id">> => ClientMsgId
    }),
    reply(Msg, <<"forward_result">>, NewBody, NewState).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%% CAS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%

execute_cas(Key, From, To, State) ->
    #{store := Store} = State,
    case maps:find(Key, Store) of
        error ->
            {<<"error">>, #{<<"code">> => 20}, State};
        {ok, From} ->
            NewState = State#{store := maps:put(Key, To, Store)},
            {<<"cas_ok">>, #{}, NewState};
        {ok, _} ->
            {<<"error">>, #{<<"code">> => 22}, State}
    end.

handle_cas(Msg, State) ->
    Body = maps:get(<<"body">>, Msg),
    Key = maps:get(<<"key">>, Body),
    Node = get_responsible_node(Key, State),
    MyId = maps:get(node_id, State),
    if Node =:= MyId ->
        From = maps:get(<<"from">>, Body),
        To = maps:get(<<"to">>, Body),
        {ResultType, ResultBody, NewState} = execute_cas(Key, From, To, State),
        reply(Msg, ResultType, ResultBody, NewState);
    true ->
        NewBody = #{
            <<"type">> => <<"redirected_cas">>,
            <<"key">> => Key,
            <<"from">> => maps:get(<<"from">>, Body),
            <<"to">> => maps:get(<<"to">>, Body),
            <<"client">> => maps:get(<<"src">>, Msg),
            <<"client_msg_id">> => maps:get(<<"msg_id">>, Body)
        },
        send_msg(Node, NewBody, State)
    end.

handle_redirected_cas(Msg, State) ->
    Body = maps:get(<<"body">>, Msg),
    Key = maps:get(<<"key">>, Body),
    From = maps:get(<<"from">>, Body),
    To = maps:get(<<"to">>, Body),
    Client = maps:get(<<"client">>, Body),
    ClientMsgId = maps:get(<<"client_msg_id">>, Body),
    {ResultType, ResultBody, NewState} = execute_cas(Key, From, To, State),
    NewBody = maps:merge(ResultBody, #{
        <<"original_type">> => ResultType,
        <<"client">> => Client, 
        <<"client_msg_id">> => ClientMsgId
    }),
    reply(Msg, <<"forward_result">>, NewBody, NewState).
