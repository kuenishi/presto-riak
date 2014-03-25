-module(ldna). %% Local Direct Node Access

%% @doc typical usage:
%% [ldna:fetch_vnode(VN, <<"b">>) || VN <- ldna:get_local_coverage(42)].

-export([get_coverage_plan/1, get_local_coverage/1,
         get_local_ring/0, fetch_vnode/2]).

% lists:flatten([riak_kv_vnode:fold({I,node()}, fun({B,K},V,Acc) -> [riak_object:from_binary(B,K,V)|Acc]end, [])||I<-riak_core_ring:my_indices(element(2, riak_core_ring_manager:get_my_ring()))]).

get_coverage_plan(ReqID) when is_integer(ReqID) ->
    riak_core_coverage_plan:create_plan(all, 3, 1, ReqID, riak_kv).

get_local_coverage(ReqID) ->
    io:format("~p~n", [ReqID]),
    {CoverageVNode, _VNodeFilters} = get_coverage_plan(ReqID),
    %% _VNodeFilters might be to process module thingy,
    %% thus it's TODO to steal from Riak Pipe codes.
    Self = node(),
    [{Index,Node} || {Index, Node} <- CoverageVNode,
                     Node =:= Self ].

get_local_ring() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    MyIndices = riak_core_ring:my_indices(Ring),
    [{Index,node()} || Index <- MyIndices].

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
