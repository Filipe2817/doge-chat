-module(connection_sup).
-behaviour(supervisor).

-export([start_link/0, start_child/1]).
-export([init/1]).

%%--------------------------------------------------------------------
%% API Functions
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
init([]) ->
    %% Dynamic supervisor: no static children specified.
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    {ok, {SupFlags, []}}.

start_child(Socket) ->
    ChildId = {client_conn, Socket},
    ChildSpec = #{
        id => ChildId,
        start => {client_conn, start_link, [Socket]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [client_conn]
    },
    supervisor:start_child(?MODULE, ChildSpec).
