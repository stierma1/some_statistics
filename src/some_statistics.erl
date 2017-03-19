-module(some_statistics).

%% API exports
-export([count/1, sum/1, max/1, min/1, average/1, type_it/1, mode/1, histogram/1, div_con_sort/1, median/1, quartile/2,
  loop_sum/0, loop_count/0, loop_average/0, loop_mode/0, loop_max/0, loop_min/0, loop_median/0,
  loop_quartile/0, loop_histogram/0, loop_div_con_sort/0, loop_var/0, var/1, loop_sample_var/0,
  sample_var/1, loop_std/0, std/1, loop_cov/0, cov/2, zip/3, loop_sample_cov/0, sample_cov/2, loop_corr/0, corr/2]).

%%====================================================================
%% API functions
%%====================================================================

type_it(Data) ->
  if
    is_map(Data) ->
      {key_values, maps:to_list(Data)} ;
    is_list(Data) ->
      {list, Data} ;
    true ->
      {list, [Data]}
  end.

loop_sum() ->
    receive
      {ReturnPid, L} -> ReturnPid ! {ok, sum(L)}, loop_sum() ;
      _ -> loop_sum()
    end.

sum({_, []}) -> 0 ;
sum({key_values, [{_, V} | T]}) -> V + sum({key_values, T}) ;
sum({list, [H | T]}) -> H + sum({list, T}) ;
sum(X) -> sum(type_it(X)).

loop_count() ->
    receive
      {ReturnPid, L} -> ReturnPid ! {ok, count(L)}, loop_count() ;
      _ -> loop_count()
    end.

count({_, L}) -> Val = lists:map(fun(_) -> 1 end, L), sum({list, Val}) ;
count(T) -> count(type_it(T)).

loop_average() ->
    receive
      {ReturnPid, L} -> ReturnPid ! {ok, average(L)}, loop_average() ;
      _ -> loop_average()
    end.

average([]) -> {cant_average, no_elements} ;
average(#{}) -> {cant_average, no_elements} ;
average({_, []}) -> {cant_average, no_elements} ;
average(T) -> sum(T) / count(T).

loop_max() ->
    receive
      {ReturnPid, L} -> ReturnPid ! {ok, max(L)}, loop_max() ;
      _ -> loop_max()
    end.

max({K, T}) -> max({K, T}, some_stats_noncompareable) ;
max(T) -> max(type_it(T), some_stats_noncompareable).

max({_, []}, some_stats_noncompareable) -> {cant_max, no_elements} ;
max({_, []}, Max) -> Max ;
max({Type, [H | T]}, some_stats_noncompareable) -> max({Type, T}, H) ;
max({list, [H | T]}, Max) ->
  if
    H > Max ->
      max({list, T}, H) ;
    true ->
      max({list, T}, Max)
  end ;
max({key_values, [{K,V} | T]}, {Key, Max}) ->
  if
    V > Max ->
      max({key_values, T}, {K, V}) ;
    true ->
      max({key_values, T}, {Key, Max})
  end.

loop_min() ->
    receive
      {ReturnPid, L} -> ReturnPid ! {ok, min(L)}, loop_min() ;
      _ -> loop_min()
    end.

min({K, T}) -> min({K, T}, some_stats_noncompareable) ;
min(T) -> min(type_it(T), some_stats_noncompareable).

min({_, []}, some_stats_noncompareable) -> {cant_max, no_elements} ;
min({_, []}, Min) -> Min ;
min({Type, [H | T]}, some_stats_noncompareable) -> min({Type, T}, H) ;
min({list, [H | T]}, Min) ->
    if
      H < Min ->
        min({list, T}, H) ;
      true ->
        min({list, T}, Min)
    end ;
min({key_values, [{K,V} | T]}, {Key, Min}) ->
    if
      V < Min ->
        min({key_values, T}, {K, V}) ;
      true ->
        min({key_values, T}, {Key, Min})
    end.

loop_mode() ->
    receive
      {ReturnPid, L} -> ReturnPid ! {ok, mode(L)}, loop_mode() ;
      _ -> loop_mode()
    end.

mode(X) ->
  {K, _} = max(histogram(X)),
  K.

loop_histogram() ->
  receive
    {ReturnPid, L} -> ReturnPid ! {ok, histogram(L)}, loop_histogram() ;
    _ -> loop_histogram()
  end.

histogram(X) -> ProtoStats = lists:map(fun (Ele) -> {Ele, 0} end, X),
  Stats = maps:from_list(ProtoStats),
  fill_histogram(X, Stats).

fill_histogram([], Stats) -> Stats ;
fill_histogram([H | T], Stats) ->
    {ok,V} = maps:find(H, Stats),
    Stats1 = Stats#{H := V + 1},
    fill_histogram(T, Stats1).

loop_div_con_sort() ->
    receive
      {ReturnPid, L} -> ReturnPid ! {ok, div_con_sort(L)}, loop_div_con_sort() ;
      _ -> loop_div_con_sort()
    end.

div_con_sort(T) ->
  {_, V} = div_con_sort(type_it(T), 16), V.

div_con_sort({list, T}, BucketSize) ->
  Len = length(T),
  if
    Len =< BucketSize ->
      {list, lists:sort(T)} ;
    true ->
      {List1, List2} = lists:split(length(T) div 2, T),
      {list, Sorted1} = div_con_sort({list, List1}, BucketSize),
      {list, Sorted2} = div_con_sort({list, List2}, BucketSize),
      {list, lists:reverse(merge(Sorted1, Sorted2))}
  end ;
div_con_sort({key_values, T}, BucketSize) ->
    Len = length(T),
    if
      Len =< BucketSize ->
        {key_values, lists:keysort(2,T)} ;
      true ->
        {List1, List2} = lists:split(length(T) div 2, T),
        {key_values, Sorted1} = div_con_sort({key_values, List1}, BucketSize),
        {key_values, Sorted2} = div_con_sort({key_values, List2}, BucketSize),
        {key_values, lists:reverse(merge(Sorted1, Sorted2))}
    end.

merge(Sorted1, Sorted2) ->
  merge(Sorted1, Sorted2, []).
merge([], Sorted2, FullSorted) ->
  lists:append(lists:reverse(Sorted2), FullSorted) ;
merge(Sorted1, [], FullSorted) ->
  lists:append(lists:reverse(Sorted1), FullSorted) ;
merge([{K1,V1} | Sorted1], [{K2,V2} | Sorted2], FullSorted) ->
    if
      V1 < V2 ->
        merge(Sorted1, [{K2,V2} | Sorted2], [{K1,V1} | FullSorted]) ;
      true ->
        merge([{K1,V1} | Sorted1], Sorted2, [{K2,V2} | FullSorted])
    end ;
merge([H1 | Sorted1], [H2 | Sorted2], FullSorted) ->
  if
    H1 < H2 ->
      merge(Sorted1, [H2 | Sorted2], [H1 | FullSorted]) ;
    true ->
      merge([H1 | Sorted1], Sorted2, [H2 | FullSorted])
  end.

loop_quartile() ->
  receive
    {ReturnPid, L, Quartile} -> ReturnPid ! {ok, quartile(L, Quartile)}, loop_quartile() ;
    _ -> loop_quartile()
  end.

quartile({_, L}, Quartile) ->
  Sorted = div_con_sort(L),
  QuartileIdx = Quartile * length(Sorted),
  Aprox = round(QuartileIdx),
  lists:nth(Aprox, Sorted);
quartile(L, Quartile) ->
  quartile(type_it(L), Quartile).


loop_median() ->
  receive
    {ReturnPid, L} -> ReturnPid ! {ok, median(L)}, loop_median() ;
    _ -> loop_median()
  end.

median(L) ->
  quartile(L, 0.5).

loop_var() ->
  receive
    {ReturnPid, L} -> ReturnPid ! {ok, var(L)}, loop_var() ;
    _ -> loop_var()
  end.

var(X) ->
  Mean = average(X),
  Val = type_it(X),
  var(Val, Mean).

var({list, List}, Mean) ->
  NewList = lists:map(fun (Ele) -> (Ele - Mean) * (Ele - Mean) end, List),
  average(NewList) ;
var({key_values, KeyVales}, Mean) ->
  NewList = lists:map(fun ({_, Ele}) -> (Ele - Mean) * (Ele - Mean) end, KeyVales),
  average(NewList).

loop_std() ->
  receive
    {ReturnPid, L} -> ReturnPid ! {ok, std(L)}, loop_std() ;
    _ -> loop_std()
  end.

std(X) ->
  math:sqrt(var(X)).

loop_sample_var() ->
  receive
    {ReturnPid, L} -> ReturnPid ! {ok, sample_var(L)}, loop_sample_var() ;
    _ -> loop_sample_var()
  end.

sample_var(X) ->
  Var = var(X),
  {_, L} = type_it(X),
  Length = length(L),
  Var * (Length / (Length - 1)).

zip([], [], Zipped) -> Zipped ;
zip([H1 | T1], [H2 | T2], Zipped) -> zip(T1, T2, [{H1, H2} | Zipped]).

loop_cov() ->
  receive
    {ReturnPid, X, Y} -> ReturnPid ! {ok, cov(X,Y)}, loop_cov() ;
    _ -> loop_cov()
  end.

cov(X, Y) ->
  XMean = average(X),
  XVal = type_it(X),
  YMean = average(Y),
  YVal = type_it(Y),
  cov(XVal, YVal, XMean, YMean).

cov({list, XList}, {list, YList}, XMean, YMean) ->
  ZippedCov = zip(XList, YList, []),
  NewList = lists:map(fun ({XEle, YEle}) -> (XEle - XMean) * (YEle - YMean) end, ZippedCov),
  average(NewList) ;
cov(_, _, _, _) -> unmatched_error.

loop_sample_cov() ->
  receive
    {ReturnPid, X, Y} -> ReturnPid ! {ok, sample_cov(X,Y)}, loop_sample_cov() ;
    _ -> loop_sample_cov()
  end.

sample_cov(X, Y) ->
  Cov = cov(X,Y),
  {_, L} = type_it(X),
  Length = length(L),
  Cov * (Length/(Length - 1)).

loop_corr() ->
  receive
    {ReturnPid, X, Y} -> ReturnPid ! {ok, corr(X,Y)}, loop_corr() ;
    _ -> loop_corr()
  end.

corr(X, Y) ->
  {_,XL} = type_it(X),
  {_,YL} = type_it(Y),
  Length = length(XL),
  ZippedCor = zip(XL, YL, []),
  ProductL = lists:map(fun ({XEle, YEle}) -> (XEle * YEle) end, ZippedCor),
  SumX = sum(X),
  SumY = sum(Y),
  Top = Length * sum(ProductL) - (SumX * SumY),
  XSquares = lists:map(fun (XEle) -> (XEle * XEle) end, XL),
  YSquares = lists:map(fun (YEle) -> (YEle * YEle) end, YL),
  Bottom = math:sqrt((Length * sum(XSquares) - (SumX * SumX)) * (Length * sum(YSquares) - (SumY * SumY))),
  (Top / Bottom).


%%====================================================================
%% Internal functions
%%====================================================================
