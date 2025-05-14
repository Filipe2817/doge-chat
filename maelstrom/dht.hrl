%%% dht implementation logic %%%

-define(BUCKETS_PER_NODE, 4).

sha1(Data) ->
    DataBin = case Data of
        B when is_binary(B) -> B;
        L when is_list(L)   -> list_to_binary(L);
        Other               -> term_to_binary(Other)
    end,
    HashBin = crypto:hash(sha, DataBin),
    BinList = binary:bin_to_list(HashBin),
    lists:flatten([ io_lib:format("~2.16.0B", [Byte]) || Byte <- BinList ]).

get_responsible_node(Key, State) ->
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

virtual_label(NodeId, I) ->
    Bin = case NodeId of
        B when is_binary(B) -> B;
        L when is_list(L)   -> list_to_binary(L);
        Other               -> term_to_binary(Other)
    end,
    BinIteration = integer_to_binary(I),
    <<Bin/binary, "#", BinIteration/binary>>.

gen_virtual_nodes_hashes(NodeId) ->
    [ sha1(virtual_label(NodeId, I)) || I <- lists:seq(1, ?BUCKETS_PER_NODE) ].
