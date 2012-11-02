%%%-------------------------------------------------------------------
%%% File     : kafka_stream_consumer_sup.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------

-module(kafka_stream_consumer).
-behaviour(gen_server).

-export([start_link/1, get_stream_function/0, get_terminate_function/0]).

-export([stream_messages/1, stop_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {conn_pid, ksr_pid , count=5, time=5000}).



%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------

start_link(KsrPid) -> 
   start_link(KsrPid, 5, 5000).

get_stream_function() -> 
   fun stream_messages/1.

get_terminate_function() -> 
   fun stop_link/1.


%%%-------------------------------------------------------------------
%%%                       INTERNAL  API FUNCTIONS
%%%-------------------------------------------------------------------


stream_messages(ConnPid) -> 
  gen_server:call(ConnPid, stream_messages).


stop_link(ConnPid) -> 

   gen_server:cast(ConnPid, stop_link).


%%%-------------------------------------------------------------------
%%%                       GEN_SERVER CB FUNCTIONS
%%%-------------------------------------------------------------------


init([ KsrPid, Count, Time]) -> 
   {ok, #state{ksr_pid=KsrPid, count=Count, time=Time}, 0}.

handle_call(get_state, _From, State) -> 
   {reply, {ok, State}, State};

handle_call(stream_messages, _From, #state{ksr_pid=KsrPid, count=Count, time=Time} = State) -> 


 try 
    lists:foreach(fun(_X) -> 
    			  
        		  Resp = kafka_sequential_reader:next_messages(KsrPid),
        		  case Resp of 
			  	     {ok, []} ->
				          throw(no_data);
                                     {ok, {[], _ }} ->
	     	  		     	  ok;
				     {ok, {Messages, Size}} ->
 			     	          throw({Messages, Size})
         		   end,
         		   receive 
			   after Time -> 
              		   	 ok       % wait for Time seconds in a loop
		           end
                    end,
                  lists:seq(1,Count)),     % loop through Count times

        {reply, {ok, no_data}, State}
   
  catch
     throw:{Messages, Size} -> {reply, {ok, {Messages, Size}}, State};
     throw:no_data ->         {reply, {ok, no_data}, State}
  end.
  


handle_cast(stop_link, State) -> 
  {stop, normal, State}.



handle_info(timeout, #state{conn_pid=undefined, ksr_pid=KsrPid} = State) -> 
  NewState = State#state{conn_pid=self()},
  link(KsrPid),
  {noreply, NewState};

handle_info(_, State) -> 
  {noreply, State}.

terminate(_Reason, _State) -> 
    ok.

code_change(_OldVsn, State, _Extra) -> 
   {ok, State}.
 


%%%-------------------------------------------------------------------
%%%                       INTERNAL  FUNCTIONS
%%%-------------------------------------------------------------------

start_link(KsrPid, Count, Time) -> 
   gen_server:start_link( ?MODULE, [ KsrPid, Count, Time], []).    
