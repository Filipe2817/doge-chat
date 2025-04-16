%%% dht implementation logic %%%

sha1(Data) ->
    DataBin = case is_list(Data) of
        true -> list_to_binary(Data);
        false -> Data
    end,
    HashBin = crypto:hash(sha, DataBin),
    BinList = binary:bin_to_list(HashBin),
    lists:flatten([ io_lib:format("~2.16.0B", [Byte]) || Byte <- BinList ]).
