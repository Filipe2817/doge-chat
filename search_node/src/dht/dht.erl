-module(dht).
-export([get_responsible_node/2, gen_virtual_nodes_hashes/1, find_successors_range/2, divide_map_to_transfer/2]).

-define(BUCKETS_PER_NODE, 3).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

sha1(Data) ->
    DataBin = case Data of
        B when is_binary(B) -> B;
        L when is_list(L)   -> list_to_binary(L);
        Other               -> term_to_binary(Other)
    end,
    HashBin = crypto:hash(sha, DataBin),
    BinList = binary:bin_to_list(HashBin),
    lists:flatten([ io_lib:format("~2.16.0B", [Byte]) || Byte <- BinList ]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_responsible_node(Key, State) -> % can be cleaner with lists:dropwhile
    #{bucket_hashes := NodeHashes} = State,
    KeyHash = sha1(Key),
    get_responsible_node_aux(KeyHash, NodeHashes, hd(NodeHashes)).

get_responsible_node_aux(_KeyHash, [], {{_, FirstNode}}) ->
    FirstNode;

get_responsible_node_aux(KeyHash, [{NodeHash, NodeId} | Rest], First) ->
    case NodeHash >= KeyHash of
        true -> NodeId;
        false -> get_responsible_node_aux(KeyHash, Rest, First)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

virtual_label(NodeId, I) ->
    Bin = case NodeId of
        B when is_binary(B) -> B;
        L when is_list(L)   -> list_to_binary(L);
        Other               -> term_to_binary(Other)
    end,
    BinIteration = integer_to_binary(I),
    <<Bin/binary, "#", BinIteration/binary>>.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gen_virtual_nodes_hashes(NodeId) ->
    [ sha1(virtual_label(NodeId, I)) || I <- lists:seq(1, ?BUCKETS_PER_NODE) ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

find_pred_succ(H, Ring) ->
    find_pred_succ(H, Ring, Ring, undefined).

find_pred_succ(H, [{RH, Id} | _], Ring, PrevHash) when RH > H ->
    % found RH > H, this is the successor
    Prev = case PrevHash of 
        undefined -> 
            {LastH, _} = lists:last(Ring),
            LastH;
        Value -> 
            Value 
    end,
    {Prev, Id};

find_pred_succ(H, [{RH, _} | T], Ring, _PrevHash) ->
    % not the successor, update the predecessor and keep looking
    find_pred_succ(H, T, Ring, RH);

find_pred_succ(_H, [], [{_, FirstId} | _], PrevHash) ->
    % end of the list, successor is the first element
    {PrevHash, FirstId}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

find_successors_range(MyHashes, RingHashes) ->
    FilteredRing = RingHashes -- MyHashes,
    lists:foldl(
        fun({H, _}, Acc) ->
            {Start, SuccId} = find_pred_succ(H, FilteredRing),
            Range = {Start, H},
            OldList = maps:get(SuccId, Acc, []),
            maps:put(SuccId, [Range | OldList], Acc)
        end,
        #{},
        MyHashes
    ).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

is_in_range(H, {Start,End}) when Start < End ->
    H > Start andalso H =< End;

is_in_range(H, {Start,End}) when Start > End ->
    H > Start orelse H =< End;

is_in_range(_,_) ->
    false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

in_any_range(H, Ranges) ->
    lists:any(fun(R) -> is_in_range(H, R) end, Ranges).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

divide_map_to_transfer(KeyMap, Ranges) ->
    maps:fold(
        fun(Key, Val, {Keep, Transfer}) ->
            H = sha1(Key),
            case in_any_range(H, Ranges) of
                true  -> {Keep, maps:put(Key, Val, Transfer)};
                false -> {maps:put(Key, Val, Keep), Transfer}
            end
        end,
        {#{}, #{}},
        KeyMap
    ).
