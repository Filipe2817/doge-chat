-module(state_manager).
-behaviour(gen_server).

%% API
-export([start_link/0, get/1, put/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

%%--------------------------------------------------------------------
%% API Functions
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% get(Key) returns the value or not_found if Key does not exist.
get(Key) ->
    gen_server:call(?MODULE, {get, Key}).

%% put(Key, Value) stores the value and returns ok.
put(Key, Value) ->
    gen_server:call(?MODULE, {put, Key, Value}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
init([]) ->
    %% Initialize with an empty map
    {ok, #{}}.

handle_call({get, Key}, _From, State) ->
    Value = maps:get(Key, State, not_found),
    {reply, Value, State};
handle_call({put, Key, Value}, _From, State) ->
	NewState = State#{Key := Value},
    {reply, ok, NewState};
handle_call(_Request, _From, State) ->
    {reply, error, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
