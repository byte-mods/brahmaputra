%% @doc Partition assignment strategies, run by the group leader.
%%
%% Members computing an assignment independently must agree, so each
%% strategy is deterministic and mirrors the Rust (and Go) implementation
%% exactly: a leader running a different algorithm from its predecessor
%% would reshuffle the whole group.
-module(brahmaputra_assignor).

-export([assign/4, range/2, roundrobin/2, sticky/3]).

-type tp() :: {binary(), integer()}.
-type member() :: {MemberId :: binary(), Topics :: [binary()]}.
-type assignment() :: #{binary() => [tp()]}.

%% @doc Dispatch on the strategy name.
-spec assign(range | roundrobin | sticky, [member()], #{binary() => [integer()]},
             #{binary() => [tp()]}) -> {ok, assignment()} | {error, term()}.
assign(range, Members, TopicPartitions, _Previous) ->
    {ok, range(Members, TopicPartitions)};
assign(roundrobin, Members, TopicPartitions, _Previous) ->
    {ok, roundrobin(Members, TopicPartitions)};
assign(sticky, Members, TopicPartitions, Previous) ->
    {ok, sticky(Members, TopicPartitions, Previous)};
assign(Other, _, _, _) ->
    {error, {unknown_assignor, Other}}.

empty(Members) -> maps:from_list([{Id, []} || {Id, _} <- Members]).

subscribes(Topics, Topic) -> lists:member(Topic, Topics).

%% @doc Each subscribed member gets a contiguous range per topic; the first
%% (partitions rem members) members take one extra.
-spec range([member()], #{binary() => [integer()]}) -> assignment().
range(Members, TopicPartitions) ->
    lists:foldl(
      fun(Topic, Acc) ->
              Partitions = maps:get(Topic, TopicPartitions),
              Subscribers = lists:sort([Id || {Id, Ts} <- Members, subscribes(Ts, Topic)]),
              range_topic(Topic, Partitions, Subscribers, Acc)
      end, empty(Members), lists:sort(maps:keys(TopicPartitions))).

range_topic(_Topic, _Partitions, [], Acc) -> Acc;
range_topic(Topic, Partitions, Subscribers, Acc0) ->
    N = length(Subscribers),
    Base = length(Partitions) div N,
    Extra = length(Partitions) rem N,
    {Acc, _, _} =
        lists:foldl(
          fun(Id, {A, Remaining, Index}) ->
                  Count = Base + case Index < Extra of true -> 1; false -> 0 end,
                  {Mine, Rest} = lists:split(Count, Remaining),
                  {maps:update_with(Id, fun(L) -> L ++ [{Topic, P} || P <- Mine] end, A),
                   Rest, Index + 1}
          end, {Acc0, Partitions, 0}, Subscribers),
    Acc.

%% @doc Deal every partition around the circle of members sorted by id,
%% skipping members not subscribed to a partition's topic.
-spec roundrobin([member()], #{binary() => [integer()]}) -> assignment().
roundrobin([], _TopicPartitions) ->
    #{};
roundrobin(Members, TopicPartitions) ->
    Circle = list_to_tuple(lists:keysort(1, Members)),
    Size = tuple_size(Circle),
    Slots = [{T, P} || T <- lists:sort(maps:keys(TopicPartitions)),
                       P <- maps:get(T, TopicPartitions)],
    {Acc, _} =
        lists:foldl(
          fun({Topic, _} = Slot, {A, Cursor}) ->
                  deal(Slot, Topic, A, Cursor, Cursor, Circle, Size)
          end, {empty(Members), 0}, Slots),
    Acc.

deal(Slot, Topic, A, Start, Cursor, Circle, Size) ->
    {Id, Topics} = element((Cursor rem Size) + 1, Circle),
    Next = Cursor + 1,
    case subscribes(Topics, Topic) of
        true ->
            {maps:update_with(Id, fun(L) -> L ++ [Slot] end, A), Next};
        false when Next - Start >= Size ->
            {A, Next}; % nobody subscribes to this topic
        false ->
            deal(Slot, Topic, A, Start, Next, Circle, Size)
    end.

%% @doc Keep members on what they hold and move only what balance requires.
-spec sticky([member()], #{binary() => [integer()]}, #{binary() => [tp()]}) -> assignment().
sticky([], _TopicPartitions, _Previous) ->
    #{};
sticky(Members, TopicPartitions, Previous) ->
    SubscribedBy = maps:from_list(Members),
    Subscribes = fun(Id, Topic) ->
                         case maps:find(Id, SubscribedBy) of
                             {ok, Ts} -> subscribes(Ts, Topic);
                             error -> false
                         end
                 end,
    PreviousIds = lists:sort(maps:keys(Previous)),
    AllSlots = [{T, P} || T <- lists:sort(maps:keys(TopicPartitions)),
                          P <- maps:get(T, TopicPartitions)],
    Holder = fun({Topic, _} = Slot) ->
                     case [Id || Id <- PreviousIds,
                                 lists:member(Slot, maps:get(Id, Previous)),
                                 Subscribes(Id, Topic)] of
                         [First | _] -> First;
                         [] -> none
                     end
             end,
    {Unassigned0, Claimed} =
        lists:foldl(fun(Slot, {U, C}) ->
                            case Holder(Slot) of
                                none -> {U ++ [Slot], C};
                                Id -> {U, [{Slot, Id} | C]}
                            end
                    end, {[], []}, AllSlots),
    Eligible = lists:sort([Id || {Id, Ts} <- Members,
                                 lists:any(fun(T) -> maps:is_key(T, TopicPartitions) end, Ts)]),
    case Eligible of
        [] ->
            empty(Members);
        _ ->
            Total = lists:sum([length(Ps) || Ps <- maps:values(TopicPartitions)]),
            N = length(Eligible),
            Base = Total div N,
            Extra = Total rem N,
            Quota = maps:from_list(
                      [{Id, Base + case I < Extra of true -> 1; false -> 0 end}
                       || {I, Id} <- lists:zip(lists:seq(0, N - 1), Eligible)]),
            {Kept, Unassigned1} =
                lists:foldl(
                  fun({Slot, Id}, {K, U}) ->
                          Held = maps:get(Id, K, []),
                          case length(Held) < maps:get(Id, Quota, 0) of
                              true -> {maps:put(Id, Held ++ [Slot], K), U};
                              false -> {K, U ++ [Slot]}
                          end
                  end, {#{}, Unassigned0}, lists:sort(Claimed)),
            Start = maps:fold(fun(Id, Held, A) ->
                                      case maps:is_key(Id, A) of
                                          true -> maps:put(Id, Held, A);
                                          false -> A
                                      end
                              end, empty(Members), Kept),
            Final = lists:foldl(
                      fun({Topic, _} = Slot, A) ->
                              Under = [Id || Id <- Eligible, Subscribes(Id, Topic),
                                             length(maps:get(Id, A)) < maps:get(Id, Quota)],
                              %% Quotas exhausted (uneven subscriptions): an
                              %% unassigned partition is a stalled one, so
                              %% fall back to any subscriber.
                              Any = [Id || Id <- Eligible, Subscribes(Id, Topic)],
                              case Under ++ Any of
                                  [Taker | _] ->
                                      maps:update_with(Taker, fun(L) -> L ++ [Slot] end, A);
                                  [] ->
                                      A
                              end
                      end, Start, lists:sort(Unassigned1)),
            maps:map(fun(_, L) -> lists:sort(L) end, Final)
    end.
