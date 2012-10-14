%%%-------------------------------------------------------------------
%%% File     : erlkafka_app.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------

-module(erlkafka_app).
-author("Milind Parikh <milindparikh@gmail.com> [http://www.milindparikh.com]").
-behaviour(application).

-export([start/2, stop/1]).

start(normal, _Args) ->
   erlkafka_root_sup:start_link(1).

stop(_State) -> 
  ok.
