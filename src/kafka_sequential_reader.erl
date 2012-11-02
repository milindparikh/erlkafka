
%%%-------------------------------------------------------------------
%%% File     : kafka_sequential_reader.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------

-module(kafka_sequential_reader).
-behaviour(gen_server).
 
-export([start_link/1, next_messages/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {conn_pid, broker, partition, topic, offset, maxsize = 1048576}).


%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------

start_link([Broker,  Topic, Partition, Offset]) -> 
   gen_server:start_link( ?MODULE, [Broker,  Topic, Partition, Offset], []).    

next_messages(ConnPid) -> 
  gen_server:call(ConnPid, next_messages).


%%%-------------------------------------------------------------------
%%%                         GEN_SERVER CB FUNCTIONS
%%%-------------------------------------------------------------------



init([Broker,  Topic, Partition, Offset]) -> 
   {ok, #state{broker=Broker,topic=Topic, partition=Partition,offset=Offset}, 0}.

handle_call(next_messages, _From, #state{broker=Broker,topic=Topic, partition=Partition,offset=Offset} = State) -> 
     Resp = kafka_simple_api:fetch(Broker, Topic, Partition, Offset),

     case Resp of 
	     {ok, []} ->
	     	  {reply, Resp, State};
             {ok, {[], _ }} ->
	     	  {reply, Resp, State};
             {ok, {_Messages, Size}} ->
	         NewState = State#state{offset=Offset+Size},
        	 {reply, Resp, NewState}
      end.

     



handle_cast(stop_link, State) -> 
  {stop, normal, State}.



handle_info(timeout, #state{conn_pid=undefined} = State) -> 
  NewState = State#state{conn_pid=self()},
  {noreply, NewState};

handle_info(_, State) -> 
  {noreply, State}.

terminate(_Reason, _State) -> 
    ok.

code_change(_OldVsn, State, _Extra) -> 
   {ok, State}.

