%%%-------------------------------------------------------------------
%% @doc search_node top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(search_node_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 0,
        period => 1
    },

    StateManagerChild = #{
        id => state_manager,
        start => {state_manager, start_link, []},
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
        start => {acceptor, start_link, [1234, state_manager]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [acceptor]
    },

    %% Order matters: state_manager first, then connection_sup, then acceptor.
	% This is because the acceptor needs to know the state_manager's PID.
    ChildSpecs = [StateManagerChild, ConnectionSupChild, AcceptorChild],
    {ok, {SupFlags, ChildSpecs}}.
