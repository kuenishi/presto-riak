%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(ldna). %% Local Direct Node Access

%% @doc typical usage:
%% [ldna:fetch_vnode(VN, <<"b">>) || VN <- ldna:get_local_coverage(42)].


%% compile:
%% > ERL_LIBS=/home/kuenishi/src/riak/deps erlc  src/main/erlang/ldna.erl
-include_lib("riak_kv/include/riak_kv_index.hrl").
-include_lib("riak_kv/include/riak_kv_vnode.hrl").

-export([get_coverage_plan/1,
         process_split/3,
         process_split/4,
         fetch_vnode/2,
         version/0]).

-spec version() -> tuple(non_neg_integer()).
version() -> {0,0,5}.

% lists:flatten([riak_kv_vnode:fold({I,node()}, fun({B,K},V,Acc) -> [riak_object:from_binary(B,K,V)|Acc]end, [])||I<-riak_core_ring:my_indices(element(2, riak_core_ring_manager:get_my_ring()))]).

%% @doc returns usual coverage plan
-spec get_coverage_plan(non_neg_integer()) ->
                               {[{non_neg_integer(), atom()}],
                                [{non_neg_integer(), [non_neg_integer()]}]}.
get_coverage_plan(ReqID) when is_integer(ReqID) ->
    riak_core_coverage_plan:create_plan(all, 3, 1, ReqID, riak_kv).

%% TODO: write small tests around this function.
%%       It works with minimal tests but 
%% @doc returns list of riak_object (internal format).
-spec process_split(binary(), {atom(), integer()}, {integer(),[integer()]}) -> list().
process_split(Bucket, VNode, FilterVNodes) ->
    {Index, _} = VNode,
    FilterVNode = proplists:get_value(Index, FilterVNodes),
    ItemFilter = fun(_) -> true end,
    Filter = riak_kv_coverage_filter:build_filter(Bucket,
                                                  ItemFilter,
                                                  FilterVNode),
    FoldFun = fun({B,K},V,Acc) when B =:= Bucket ->
                      case Filter(K) of
                          true ->
                              [riak_object:from_binary(B,K,V)|Acc];
                          false ->
                              Acc
                      end;
                 (_,_,Acc) ->
                      Acc
              end,
    riak_kv_vnode:fold(VNode, FoldFun, []).


%% @doc returns list of riak_object (internal format).
-spec process_split(binary(), {atom(), integer()},
                    {integer(),[integer()]},
                    {binary(), term()}) -> list().
process_split(Bucket, VNode, FilterVNodes,
              Q0) ->
    %%riak_core_coverage_plan:create_plan(all, 3, 1, ReqID, riak_kv).

    Query = build_query(Q0, ?KV_INDEX_Q{}),
    ReqID = make_req_id(),
    riak_core_vnode_master:coverage(
      riak_kv_index_fsm:req(Bucket, none, Query),
      VNode,
      FilterVNodes,
      {raw, ReqID, self()},
      riak_kv_vnode_master),

    keysend_loop(ReqID, VNode, VNode, []).

build_query({eq, <<"$key">>, Val}, Q) ->
    Q?KV_INDEX_Q{
        start_key= Val,
        filter_field= <<"$key">>, % :: binary() | undefined,
        start_term= Val, %% :: binary() | undefined, %% Note, in a $key query, start_key==start_term
        end_term= Val, %% :: binary() | undefined, %% Note, in an eq query, start==end
        return_terms=false %% :: boolean(), %% Note, should be false for an equals query
       };
build_query({range, <<"$key">>, Start, End}, Q) ->
    Q?KV_INDEX_Q{
       start_key= Start,
       filter_field= <<"$key">>, start_term= Start,
       end_term= End, return_terms=false };
build_query({range, Field, Start, End}, Q) ->
    Q?KV_INDEX_Q{
        filter_field= Field, start_term= Start,
        end_term= End, return_terms=false};
build_query({eq, Field, Val}, Q) ->
    Q?KV_INDEX_Q{
        filter_field= Field, start_term= Val,
        end_term= Val, return_terms=false}.


%% vnode, bucket name => all riak objects in local
%% TODO: enable it to specify bucket filters, and 2i
%% as this is 'coverage' operation, no quorum and
%% no inter-node access should occur.
fetch_vnode(VNode, Bucket) ->
    riak_kv_vnode:fold(VNode,
                       fun({B,K},V,Acc) when B =:= Bucket ->
                               [riak_object:from_binary(B,K,V)|Acc];
                          (_,_,Acc) ->
                               Acc
                       end,
                       []).



%% copied riak_core, riak_kv internal functions

%% riak_kv_pipe_index
keysend_loop(ReqId, Partition, Vnode, Acc) ->
    receive
        {ReqId, {error, _Reason} = ER} ->
            ER;
        {ReqId, {From, Bucket, Keys}} ->
            _ = riak_kv_vnode:ack_keys(From),
            Objects = 
                lists:foldl(fun(Key, List) ->
                                    {ok, Obj} = try_partition(Bucket, Key, Vnode, none),
                                    [Obj|List]
                            end, [], Keys),
            keysend_loop(ReqId, Partition, Vnode, Objects ++ Acc);

        {ReqId, {Bucket, Keys}} ->
            Objects = 
                lists:foldl(fun(Key, List) ->
                                    {ok, Obj} = try_partition(Bucket, Key, Vnode, none),
                                    [Obj|List]
                            end, [], Keys),
            keysend_loop(ReqId, Partition, Vnode, Objects ++ Acc);

        {ReqId, done} ->
            {ok, Acc}
    end.


% riak_kv_pipe_get
try_partition(Bucket, Key, Vnode, _FittingDetails) ->
    ReqId = make_req_id(),
    %% Start = os:timestamp(),
    riak_core_vnode_master:command(
      Vnode,
      ?KV_GET_REQ{bkey={Bucket, Key}, req_id=ReqId},
      {raw, ReqId, self()},
      riak_kv_vnode_master),
    receive
        {ReqId, {r, {ok, Obj}, _, _}} ->
            {ok, Obj};
        {ReqId, {r, {error, _} = Error, _, _}} ->
            Error
    end.

make_req_id() ->
    erlang:phash2({self(), os:timestamp()}). % stolen from riak_client

%% riak_core_vnode_master:
%% Make a request record - exported for use by legacy modules
%% -spec make_coverage_request(vnode_req(), keyspaces(), sender(), partition()) -> #riak_coverage_req_v1{}.
%% make_coverage_request(Request, KeySpaces, Sender, Index) ->
%%     #riak_coverage_req_v1{index=Index,
%%                           keyspaces=KeySpaces,
%%                           sender=Sender,
%%                           request=Request}.

%% cast({VMaster, Node}, Req=?COVERAGE_REQ{index=Idx}) ->
%%     Mod = vmaster_to_vmod(VMaster),
%%     Proxy = reg_name(Mod, Idx, Node),
%%     gen_fsm:send_event(Proxy, Req).

%% %% Given atom 'riak_kv_vnode_master', return 'riak_kv_vnode'.
%% vmaster_to_vmod(VMaster) ->
%%     L = atom_to_list(VMaster),
%%     list_to_atom(lists:sublist(L,length(L)-7)).

%% %% riak_core_vnode_proxy
%% reg_name(Mod, Index) ->
%%     ModBin = atom_to_binary(Mod, latin1),
%%     IdxBin = list_to_binary(integer_to_list(Index)),
%%     AllBin = <<$p,$r,$o,$x,$y,$_, ModBin/binary, $_, IdxBin/binary>>,
%%     binary_to_atom(AllBin, latin1).

%% reg_name(Mod, Index, Node) ->
%%     {reg_name(Mod, Index), Node}.
