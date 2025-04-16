%%%-------------------------------------------------------------------
%% @doc search_node public API
%% @end
%%%-------------------------------------------------------------------

-module(search_node_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    search_node_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
