%%% @author Vladimir G. Sekissov <eryx67@gmail.com>
%%% @copyright (C) 2016, Vladimir G. Sekissov
%%% @doc Pull-like simple piping.
%%% Consumer initiates pipe and direct flow by sending to producer seriese of
%%% `{more, self()}'.
%%% Producer answers with `{just, any()}' or `nothing' to terminate
%%% pipe.
%%% @end
%%% Created :  5 Sep 2016 by Vladimir G. Sekissov <eryx67@gmail.com>

-module(plain_pipe).

-export([yield/1, await/1, await/2, yield_fsm/1, await_fsm/1, await_fsm/2]).
-export([socket_pipe/4, message_src/1, message_sink/2, fun_src/2]).
-export([list_pipe/1, pipe_foldmap/3, pipe_filter/2, pipe_filter/3, pipe_timeout/2]).

-export_type([maybe/1]).

-include_lib("plain_fsm/include/plain_fsm.hrl").
-include("plain_pipe.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type maybe(A) :: nothing | {just, A}.
-type accumulator() :: any().
-type millisecond() :: pos_integer().

-record(socket_state, {transport
                      , socket
                      , producer_pid
                      , consumer_pid
                      , receive_timeout = infinity
                      , receive_timeout_ref
                      }).

%% @doc Send data to consumer
%% @end
-spec yield(maybe(any())) -> ok.
yield(Msg = ?FINISH) ->
    receive
        ?ASK_MORE(Consumer) ->
            Consumer ! {self(), Msg},
            ok
    end;
yield(Msg = ?DATA(_)) ->
    receive
        ?ASK_MORE(Consumer) ->
            Consumer ! {self(), Msg},
            ok
    end.

-spec await(pid()) -> maybe(any()).
await(Producer) ->
    await(Producer, infinity).

%% @doc Ask producer for data
%% @end
-spec await(pid(), millisecond()) -> maybe(any()).
await(Producer, Timeout) ->
    Producer ! ?ASK_MORE(self()),
    receive
        {Producer, Msg = ?FINISH} ->
            Msg;
        {Producer, Msg = ?DATA(_)} ->
            Msg
    after Timeout ->
            exit(Timeout)
    end.

%% @doc Ask producer for data in context of `plain_fsm'
%% @see plain_fsm
%% @end
-spec yield_fsm(maybe(any())) -> ok.
yield_fsm(Msg = ?FINISH) ->
    plain_fsm:extended_receive(
      receive
          ?ASK_MORE(Consumer) ->
              Consumer ! {self(), Msg},
              ok
      end);
yield_fsm(Msg = ?DATA(_)) ->
    plain_fsm:extended_receive(
      receive
          ?ASK_MORE(Consumer) ->
              Consumer ! {self(), Msg},
              ok
      end).

%% @doc Ask producer for data in context of `plain_fsm'
%% @end
-spec await_fsm(pid()) -> maybe(any()).
await_fsm(Producer) ->
    await_fsm(Producer, infinity).

await_fsm(Producer, Timeout) ->
    await_fsm_1({Producer, Timeout}).

await_fsm_1({Producer, Timeout}) ->
    Producer ! ?ASK_MORE(self()),
    plain_fsm:extended_receive(
      receive
          {Producer, Msg = ?FINISH} ->
              Msg;
          {Producer, Msg = ?DATA(_)} ->
              Msg
      after Timeout ->
              exit(timeout)
      end).


%% @doc message producer without flow control.
%% It recends messages from `Parent'.
%% @end
-spec message_src(pid() | undefined) -> ok.
message_src(Parent) ->
    receive
        ?DATA_MSG(_, Data) when Parent == undefined ->
            yield(?DATA(Data)),
            message_src(Parent);
        ?DATA_MSG(Parent, Data) ->
            yield(?DATA(Data)),
            message_src(Parent);
        ?FINISH_MSG(_) when Parent == undefined ->
            yield(?FINISH),
            ok;
        ?FINISH_MSG(Parent) ->
            yield(?FINISH),
            ok
    end.

%% @doc message producer without flow control.
%% It recends messages from `Parent'.
%% @end
-spec fun_src(fun((accumulator()) -> maybe(any())), accumulator()) -> ok.
fun_src(Fn, Acc) ->
    {Msg, NewAcc} = Fn(Acc),
    yield(Msg),
    case Msg of
        ?FINISH ->
            ok;
        ?DATA(_) ->
            fun_src(Fn, NewAcc)
    end.

%% @doc Terminal consumer for pipe. `Hlr' is used for message processing.
%% @end
-spec message_sink(pid(), Hlr::fun ((any()) -> ok)) -> ok.
message_sink(Producer, Hlr) ->
    case await(Producer) of
        ?DATA(Data) ->
            Hlr(Data),
            message_sink(Producer, Hlr);
        ?FINISH ->
            ok
    end.

%% @doc Just convert list batches to individual messages
%% @end
-spec list_pipe(pid()) -> ok.
list_pipe(Producer) ->
    case await(Producer) of
        ?DATA(Data) ->
            ok = list_pipe_produce(Data),
            list_pipe(Producer);
        ?FINISH ->
            yield(?FINISH),
            ok
    end.

list_pipe_produce([]) ->
    ok;
list_pipe_produce([V|Rst]) ->
    yield(?DATA(V)),
    list_pipe_produce(Rst).

-spec pipe_foldmap(pid(), fun((any(), accumulator()) -> {any(), accumulator()}), accumulator()) -> ok.
pipe_foldmap(Producer, Fn, Acc) ->
    case await(Producer) of
        ?DATA(Data) ->
            {V, NewAcc} = Fn(Data, Acc),
            yield(?DATA(V)),
            pipe_foldmap(Producer, Fn, NewAcc);
        ?FINISH ->
            yield(?FINISH),
            ok
    end.


-spec pipe_timeout(pid(), millisecond()) -> ok.
pipe_timeout(Producer, Timeout) ->
    await(Producer, Timeout).

-spec pipe_filter(pid(), fun((any()) -> boolean())) -> ok.
pipe_filter(Producer, Fn) ->
    pipe_filter(Producer, fun (R, {}) -> {Fn(R), {}} end, {}).

-spec pipe_filter(pid(), fun((any(), any()) -> {boolean(), any()}), any()) -> ok.
pipe_filter(Producer, Fn, Acc) ->
    case await(Producer) of
        ?DATA(Data) ->
            NxtAcc =
                case Fn(Data, Acc) of
                    {true, Acc1} ->
                        yield(?DATA(Data)),
                        Acc1;
                {false, Acc1} ->
                    Acc1
            end,
            pipe_filter(Producer, Fn, NxtAcc);
        ?FINISH ->
            yield(?FINISH),
            ok
    end.

%% @doc Create pipe with socket. It acts as a consumer for `Producer'
%% and as a producer. Connection is terminated after `RecvTimeout'
%% idle timeout. `Transport' module must support `ranch' transport
%% protocol.
%% @end
-spec socket_pipe(Transport::module(), Socket::pid(),
                  Producer::pid(), RecvTimeout::integer()) -> ok.
socket_pipe(Transport, Socket, Producer, RecvTimeout) ->
    State = #socket_state{transport = Transport, socket = Socket,
                          producer_pid = Producer,
                          receive_timeout = RecvTimeout
                         },
    Producer ! ?ASK_MORE(self()),
    socket_pipe_loop(State).

socket_pipe_loop(S = #socket_state{transport = T, socket = Soc,
                                   producer_pid = Prod,
                                   consumer_pid = Cons
                                  }) ->
    {OK, Closed, Error} = T:messages(),
    receive
        %% Consumer part
        ?ASK_MORE(Pid) ->
            ok = T:setopts(Soc, [{active, once}]),
            S1 = set_receive_timeout(S),
            socket_pipe_loop(S1#socket_state{consumer_pid = Pid});
        {OK, Soc, Data} ->
            Cons ! ?DATA_MSG(Data),
            S1 = clear_receive_timeout(S),
            socket_pipe_loop(S1);
        {Closed, Soc} ->
            Cons ! ?FINISH_MSG(),
            ok;
        {Error, Soc, Reason} ->
            error(Reason);
        {receive_timeout, Soc} ->
            error(timeout);
        %% Producer part
        ?DATA_MSG(Prod, {file, FilePath}) ->
            FS = filelib:file_size(FilePath),
            {ok, FS} = T:sendfile(Soc, FilePath),
            Prod ! ?ASK_MORE(self()),
            socket_pipe_loop(S);
        ?DATA_MSG(Prod, Data) ->
            ok = T:send(Soc, Data),
            Prod ! ?ASK_MORE(self()),
            socket_pipe_loop(S);
         ?FINISH_MSG(Prod) ->
            socket_pipe_loop(S#socket_state{producer_pid = undefined});
         ?FINISH_MSG(_) ->
            error(unknown_producer)
    end.

set_receive_timeout(S = #socket_state{receive_timeout = infinity}) ->
    S;
set_receive_timeout(S = #socket_state{socket = Soc, receive_timeout = RT}) ->
    S1 = clear_receive_timeout(S),
    RTRef = erlang:send_after(RT, self(), {receive_timeout, Soc}),
    S1#socket_state{receive_timeout_ref = RTRef}.

clear_receive_timeout(S = #socket_state{receive_timeout_ref = undefined}) ->
    S;
clear_receive_timeout(S = #socket_state{receive_timeout_ref = RTRef}) ->
    erlang:cancel_timer(RTRef),
    S#socket_state{receive_timeout_ref = undefined}.
