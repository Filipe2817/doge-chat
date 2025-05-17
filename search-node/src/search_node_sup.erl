%%%-------------------------------------------------------------------
%% @doc search_node top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(search_node_sup).

-behaviour(supervisor).

-export([start_link/4]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Addr, Port, NodeName, KnownNodes) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {Addr, Port, NodeName, KnownNodes}).

init({Addr, Port, NodeName, KnownNodes}) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 0,
        period => 1
    },

    StateManagerChild = #{
        id => state_manager,
        start => {state_manager, start_link, [NodeName, Addr, Port, KnownNodes]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [state_manager]
    },

    ConnectionSupChild = #{
        id => connection_sup,
        start => {connection_sup, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => supervisor,
        modules => [connection_sup]
    },

    AcceptorChild = #{
        id => acceptor,
        start => {acceptor, start_link, [Addr, Port]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [acceptor]
    },

    %% Order matters: state_manager first, then connection_sup, then acceptor.
	% This is because the acceptor needs to know the state_manager's PID.
    ChildSpecs = [StateManagerChild, ConnectionSupChild, AcceptorChild],
    {ok, {SupFlags, ChildSpecs}}.
