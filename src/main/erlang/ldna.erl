-module(ldna). %% Local Direct Node Access

%% @doc typical usage:
%% [ldna:fetch_vnode(VN, <<"b">>) || VN <- ldna:get_local_coverage(42)].

-export([get_coverage_plan/1,
         process_split/3,
         fetch_vnode/2]).

% lists:flatten([riak_kv_vnode:fold({I,node()}, fun({B,K},V,Acc) -> [riak_object:from_binary(B,K,V)|Acc]end, [])||I<-riak_core_ring:my_indices(element(2, riak_core_ring_manager:get_my_ring()))]).

%% @doc returns usual coverage plan
-spec get_coverage_plan(non_neg_integer()) ->
                               {[{non_neg_integer(), atom()}],
                                [{non_neg_integer(), [non_neg_integer()]}]}.
get_coverage_plan(ReqID) when is_integer(ReqID) ->
    riak_core_coverage_plan:create_plan(all, 3, 1, ReqID, riak_kv).

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
