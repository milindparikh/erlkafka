%%%-------------------------------------------------------------------
%%% File     : erlkafka.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------
 
-module(erlkafka).
-author('Milind Parikh <milindparikh@gmail.com>').
-include("erlkafka.hrl").


-export([get_kafka_stream_consumer/4, uuid/0]).
-import(uuid).


%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------


uuid() -> 
   uuid:v4().



get_kafka_stream_consumer(Broker, Topic, Partition, Offset) -> 
   {A1, A2, A3} = now(),
   random:seed(A1, A2, A3),

   UuidKSR_1 = uuid(),
   UuidKSR = uuid:to_string(UuidKSR_1),

   UuidKSC_1 = uuid(),
   UuidKSC = uuid:to_string(UuidKSC_1),

    
   {ok, KsrPid} = 
   	supervisor:start_child(
		kafka_stream_consumer_sup, 
		{
		  UuidKSR,                  
                  {
                      kafka_sequential_reader,
		      start_link, 
		      [[Broker, Topic, Partition, Offset]]
                  },
		  temporary,   % never restart
                  brutal_kill, 
		  worker,
		  [kafka_sequential_reader]
                }),

    {ok, KscPid} = 
            supervisor:start_child(
			kafka_stream_consumer_sup,  
			{
	                     UuidKSC, 
                             {
				kafka_stream_consumer, 
			     	start_link,
                             	[KsrPid]
			     },

		  	     temporary,   % never restart
			     brutal_kill, 
		  	     worker,
		  	     [kafka_stream_consumer]
                        }),



   {kafka_stream_consumer:get_stream_function(), 
    kafka_stream_consumer:get_terminate_function(), 
    KscPid}.

           



                   
   