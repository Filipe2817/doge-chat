-module(search_node_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    %% Start your top-level supervisor.
    search_node_sup:start_link().

stop(_State) ->
    ok.
