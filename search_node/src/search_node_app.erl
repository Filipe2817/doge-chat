-module(search_node_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Addr}       = application:get_env(search_node, addr),
    {ok, Port}       = application:get_env(search_node, port),
    {ok, NodeName}   = application:get_env(search_node, node_name),
    {ok, KnownNodes} = application:get_env(search_node, known_nodes),
    search_node_sup:start_link(Addr, Port, NodeName, KnownNodes).

stop(_State) ->
    ok.
