%%%-------------------------------------------------------------------
%%% File     : kafka_stream_consumer_sup.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------


-module(kafka_stream_consumer_sup).
-author("Milind Parikh <milindparikh@gmail.com> [http://www.milindparikh.com]").
-behaviour(supervisor).


-export([start_link/0

	]).
-export([init/1]).


-define(DEFAULT_POOL_COUNT, 5).


%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------


start_link() -> 

        supervisor:start_link({local, ?MODULE}, 
			      ?MODULE, []).

	 


%%%-------------------------------------------------------------------
%%%                         SUPERVISOR CB FUNCTIONS
%%%-------------------------------------------------------------------




init([]) -> 

  RestartStrategy = {one_for_one, 0, 1},
  Children = [],


  {ok, {RestartStrategy, Children}}.



%%%-------------------------------------------------------------------
%%%                         INTERNAL  FUNCTIONS
%%%-------------------------------------------------------------------



