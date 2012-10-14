%%%-------------------------------------------------------------------
%%% File     : kafka_server.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------

-module(kafka_server).
-author('Milind Parikh <milindparikh@gmail.com>').
-behaviour(gen_server).
-include("erlkafka.hrl").


-export([start_link/0, start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {socket, maxsize = ?MAX_MSG_SIZE}).

%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------


start_link() -> 
    start_link(['127.0.0.1', 9092]). 


start_link([Host, Port]) -> 
   gen_server:start_link( ?MODULE, [Host, Port], []).



%%%-------------------------------------------------------------------
%%%                         GEN_SERVER CB FUNCTIONS
%%%-------------------------------------------------------------------

init([Host, Port]) -> 
    {ok, Socket} = gen_tcp:connect(Host, Port,
                                   [binary, {active, false}, {packet, raw}]),
   {ok, #state{socket=Socket}, 0}.


handle_call({request_with_response, Req}, _From, State) -> 

  ok = gen_tcp:send(State#state.socket, Req),


  case gen_tcp:recv(State#state.socket, 6) of
        {ok, <<2:32/integer, 0:16/integer>>} ->
            {reply, {ok, []}, State};
        {ok, <<L:32/integer, 0:16/integer>>} ->
            {ok, Data} = gen_tcp:recv(State#state.socket, L-2),
            {Messages, Size} = kafka_protocol:parse_messages(Data),
            {reply, {ok, {Messages, Size}}, State};

        {ok, B} ->
            {reply, {error, B}, State}
    end;

handle_call({request_with_response_offset, Req}, _From, State) -> 

  ok = gen_tcp:send(State#state.socket, Req),


  case gen_tcp:recv(State#state.socket, 6) of
        {ok, <<2:32/integer, 0:16/integer>>} ->
            {reply, {ok, []}, State};
        {ok, <<L:32/integer, 0:16/integer>>} ->
            {ok, Data} = gen_tcp:recv(State#state.socket, L-2),
            Offsets = kafka_protocol:parse_offsets(Data),
            {reply, {ok, Offsets}, State};

        {ok, B} ->
            {reply, {error, B}, State}
    end;



handle_call({request, Req}, _From, State) -> 

  ok = gen_tcp:send(State#state.socket, Req),
  {reply, ok, State}.



handle_cast(stop_link, State) -> 
  {stop, normal, State}.

handle_info(_, State) -> 
  {noreply, State}.

terminate(_Reason, _State) -> 
    ok.

code_change(_OldVsn, State, _Extra) -> 
   {ok, State}.

