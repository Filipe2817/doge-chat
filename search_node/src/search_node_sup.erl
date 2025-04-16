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

	AcceptorChild = #{
		id => acceptor,
		start => {acceptor, start_link, [1234]},
		restart => permanent,
		shutdown => 5000,
		type => worker,
		modules => [acceptor]
	},

    ChildSpecs = [AcceptorChild],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
