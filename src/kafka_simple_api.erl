%%%-------------------------------------------------------------------
%%% File     : kafka_simple_api.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------

-module(kafka_simple_api).
-author('Milind Parikh <milindparikh@gmail.com>').


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("erlkafka.hrl").

-export([produce/4, multi_produce/2, fetch/4, multi_fetch/2, offset/5]).
-export([get_list_of_brokers/0, get_list_of_broker_partitions/1]).

%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------

produce(Broker, Topic, Partition, Messages) -> 
   Req = kafka_protocol:produce_request (Topic, Partition, Messages),
   call({Broker, request, Req}).  % a produce in Kafka 0.7 has no response
     
multi_produce(Broker, TopicPartitionMessages) -> 
   Req = kafka_protocol:multi_produce_request(TopicPartitionMessages),
   call({Broker, request, Req}). % a produce in Kafka 0.7 has no response

fetch (Broker, Topic, Partition, Offset) -> 
   Req = kafka_protocol:fetch_request(Topic, Offset, Partition, ?MAX_MSG_SIZE),
   call({Broker, request_with_response, Req}).

multi_fetch(Broker, TopicPartitionOffsets) -> 
   Req = kafka_protocol:multi_fetch_request (TopicPartitionOffsets), 
   call({Broker, request_with_response, Req}).

offset(Broker, Topic, Partition, Time, MaxNumberOfOffsets) ->
   Req = kafka_protocol:offset_request(Topic, Partition, Time, MaxNumberOfOffsets), 
   call({Broker, request_with_response_offset, Req}).

get_list_of_brokers() ->
   
   kafka_protocol:get_list_of_brokers(
	application:get_env(erlkafka_app, enable_autodiscovery),   
	application:get_env(erlkafka_app, kafka_brokers),
	application:get_env(erlkafka_app, kafka_prefix)
   ).

get_list_of_broker_partitions(Topic) -> 
   kafka_protocol:get_list_of_broker_partitions(
	application:get_env(erlkafka_app, enable_autodiscovery),   
	application:get_env(erlkafka_app, kafka_brokers),
	application:get_env(erlkafka_app, kafka_prefix),
	Topic
   ).

%%%-------------------------------------------------------------------
%%%                         INTERNAL FUNCTIONS
%%%-------------------------------------------------------------------


call({Broker, request, Req}) -> 
   case kafka_server_sup:get_random_broker_instance_from_pool(Broker) of 
       {error, _} -> 
           {error, unable_to_get_broker_instance_from_pool};

        {BrokerInstancePid, _BrokerInstanceId} -> 
              gen_server:call(BrokerInstancePid, {request, Req})
   end;


call({Broker, request_with_response_offset, Req}) -> 
   case kafka_server_sup:get_random_broker_instance_from_pool(Broker) of 
       {error, _} -> 
           {error, unable_to_get_broker_instance_from_pool};

        {BrokerInstancePid,_BrokerInstanceId}  -> 
              gen_server:call(BrokerInstancePid, {request_with_response_offset, Req})
   end;

call({Broker, request_with_response, Req}) -> 
   case kafka_server_sup:get_random_broker_instance_from_pool(Broker) of 
       {error, _} -> 
           {error, unable_to_get_broker_instance_from_pool};

        {BrokerInstancePid,_BrokerInstanceId}  -> 
              gen_server:call(BrokerInstancePid, {request_with_response, Req})
   end.



       

%%%-------------------------------------------------------------------
%%%                         TEST FUNCTIONS
%%%-------------------------------------------------------------------

-ifdef(TEST).

get_list_of_brokers_test()->
    get_list_of_brokers().


produce_test() -> 
    BrokerId = 0,
    Topic = <<"test">>,
    Partition = 0,
    Messages = [<<"hi">>, <<"there">>],
    produce(BrokerId, Topic, Partition, Messages).    

multi_produce_test() -> 
     BrokerId = 0,
     TopicPartitionMessages = [ 
     			        {
				 <<"test1">>,         %Topic
				 0,                   % partition 
				 [{1,0, <<"hi">> },   % {Magic, Compression, Msg}
                                  {1,0, <<"there">>}]
                                }, 
     			        {
				 <<"test2">>,         %Topic
				 0,                   % partition 
				 [{1,0, <<"hello">> },   % {Magic, Compression, Msg}
                                  {1,0, <<"world">>}]
                                }
			      ],
       multi_produce(BrokerId, TopicPartitionMessages).



fetch_test () -> 
    BrokerId = 0,
    Topic = <<"test">>,
    Partition = 0,
    Offset = 0,
    fetch(BrokerId, Topic, Partition, Offset).

multi_fetch_test() ->
    BrokerId = 0,
    TopicPartitionOffsets = [{<<"test">>, 0, 0, ?MAX_MSG_SIZE}, {<<"test2">>, 0,0, ?MAX_MSG_SIZE}],
    multi_fetch(BrokerId,TopicPartitionOffsets). 


offset_test() -> 
    BrokerId = 0,
    Topic = <<"test">>,
    Partition = 0,
    Time = -1,
    MaxNumberOfOffsets = 10,
    offset(BrokerId, Topic, Partition, Time, MaxNumberOfOffsets).

-endif.
