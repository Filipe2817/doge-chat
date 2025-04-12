-module(search_server).
-export([start/0, stop/0]).
-define(Port, 9000).

start() ->
  Opts = [binary, {active, once}, {packet, line}, {reuseaddr, true}],
  {ok, SSock} = gen_tcp:listen(?Port, Opts),
  register(?MODULE, self()),
  spawn(fun() -> acceptor(SSock) end),
  io:format("Search server started on port ~p~n", [?Port]),
  receive
    stop ->
      io:format("Stopping search server~n", []),
      gen_tcp:close(SSock),
      exit(normal)
  end.

stop() ->
  ?MODULE ! stop,
  init:stop(). % Clean shutdown Erlang node

acceptor(SSock) ->
  case gen_tcp:accept(SSock) of
    {ok, Sock} ->
      spawn(fun() -> acceptor(SSock) end),
      io:format("New handler [~p:~p]~n", [self(), Sock]),
      handler(Sock);
    {error, closed} ->
      io:format("Server socket closed~n", [])
  end.

handler(Sock) ->
  Self = self(),
  receive
    {tcp, Sock, Data} ->
      inet:setopts(Sock, [{active, once}]),
      gen_tcp:send(Sock, Data),
      handler(Sock);
    {tcp_closed, Sock} ->
      io:format("[~p:~p] Connection closed~n", [Self, Sock]),
      ok;
    {tcp_error, Sock, Reason} ->
      io:format("[~p:~p] TCP error: ~p~n", [Self, Sock, Reason]),
      ok
  end.
