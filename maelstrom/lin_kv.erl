#!/usr/bin/env escript

-module(lin_kv).
-export([main/1]).
% escript does not support multiple modules
% the only solution is to include header files with the functions
% having unused functions may cause warnings that break maelstrom execution
-include("helpers.hrl").
%-include("dht.hrl").

main(_Args) ->
    loop(#{
        node_id => undefined,
        node_ids => [],
        msg_id => 0,
        store => #{}
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
                    print("Received: ~p~n", [Msg]),
                    NewState = handle_message(Msg, State),
                    loop(NewState)
            end
    end.

handle_message(#{<<"body">> := Body} = Msg, State) ->
    Type = maps:get(<<"type">>, Body, <<>>),
    case Type of
        <<"init">> -> handle_init(Msg, State);
        <<"read">> -> handle_read(Msg, State);
        <<"write">> -> handle_write(Msg, State);
        <<"cas">> -> handle_cas(Msg, State);
        _Other ->
            print("Unknown message type: ~p~n", [Type]),
            State
    end.

handle_init(Msg, State) ->
    Body = maps:get(<<"body">>, Msg),
    NewState = State#{
        node_id := maps:get(<<"node_id">>, Body),
        node_ids := maps:get(<<"node_ids">>, Body, [])
    },
    reply(Msg, <<"init_ok">>, #{}, NewState).

handle_read(Msg, State) ->
    Body = maps:get(<<"body">>, Msg),
    Key = maps:get(<<"key">>, Body, undefined),
    #{store := Store} = State,
    Value = maps:get(Key, Store, null),
    reply(Msg, <<"read_ok">>, #{<<"value">> => Value}, State).

handle_write(Msg, State) ->
    Body = maps:get(<<"body">>, Msg),
    Key = maps:get(<<"key">>, Body, undefined),
    Value = maps:get(<<"value">>, Body, undefined),
    #{store := Store} = State,
    NewState = State#{store := maps:put(Key, Value, Store)},
    reply(Msg, <<"write_ok">>, #{}, NewState).

handle_cas(Msg, State) ->
    Body = maps:get(<<"body">>, Msg),
    Key = maps:get(<<"key">>, Body, undefined),
    From = maps:get(<<"from">>, Body, undefined),
    To = maps:get(<<"to">>, Body, undefined),
    #{store := Store} = State,
    case maps:find(Key, Store) of
        error ->
            reply(Msg, <<"error">>, #{<<"code">> => 20}, State);
        {ok, From} ->
            NewState = State#{store := maps:put(Key, To, Store)},
            reply(Msg, <<"cas_ok">>, #{}, NewState);
        {ok, _} ->
            reply(Msg, <<"error">>, #{<<"code">> => 22}, State)
    end.








%%neighbors = []
%%kv = {}
%%
%%bucket_hashes = []
%%
%%def find_responsible_node(key):
%%    global bucket_hashes
%%    key_hash = hashlib.sha1(str(key).encode()).hexdigest()
%%    for i in range(len(bucket_hashes) - 1):
%%        if key_hash > bucket_hashes[i][0]:
%%            return bucket_hashes[i + 1][1]
%%    return bucket_hashes[0][1]
%%
%%@handler
%%def init(msg):
%%    global bucket_hashes
%%    n.init(msg)
%%    bucket_hash = hashlib.sha1(str(node_id()).encode()).hexdigest()
%%    bucket_hashes.append((bucket_hash,node_id()))
%%    for node in node_ids():
%%        if node != node_id():
%%            send(node, type='init_buckets', bucket_hash=bucket_hash)
%%            
%%@handler
%%def init_buckets(msg):
%%    global bucket_hashes
%%    bucket_hash = msg.body.bucket_hash
%%    bucket_hashes.append((bucket_hash, msg.src))
%%    bucket_hashes.sort()
%%
%%
%%@handler
%%def read(msg):
%%    key = msg.body.key
%%    node = find_responsible_node(key)
%%    if node == node_id():
%%        value = kv.get(key, None)
%%        reply(msg, type='read_ok', value=value)
%%    else:
%%        send_with_other_src(msg.src, node, type='read', key=key)
%%
%%@handler
%%def write(msg):
%%    key = msg.body.key
%%    value = msg.body.value
%%
%%    node = find_responsible_node(key)
%%
%%    if node == node_id():
%%        kv[key] = value
%%        reply(msg, type='write_ok')
%%    else:
%%        send_with_other_src(msg.src, node, type='write', key=key, value=value)
%%
%%@handler
%%def cas(msg):
%%    key = msg.body.key
%%    from_value = getattr(msg.body, 'from')
%%    to = msg.body.to
%%
%%    node = find_responsible_node(key)
%%
%%    if node == node_id():
%%        key = msg.body.key
%%        from_value = getattr(msg.body, 'from')
%%        to = msg.body.to
%%        if key not in kv:
%%            reply(msg, type='error', code=20, key=msg.body.key)
%%            return
%%        if kv[key] != from_value:
%%            reply(msg, type='error', code=22, key=msg.body.key)
%%            return
%%        kv[key] = to
%%        reply(msg, type='cas_ok')
%%    else:
%%        body = {"from": from_value, "to": to}
%%        send_with_other_src(msg.src, node, type='cas', key=key, **body)
%%
%%def send_with_other_src(src, dest, body={}, /, **kwds):
%%    global _msg_id
%%    _msg_id += 1
%%    if isinstance(body, dict):
%%        body = body.copy()
%%    else:
%%        body = dict(vars(body))
%%    body.update(kwds, msg_id=_msg_id)
%%    msg = dict(src=src, dest=dest, body=body)
%%    data = json.dumps(msg, default=vars)
%%    log("Sent " + data)
%%    print(data, flush=True)


