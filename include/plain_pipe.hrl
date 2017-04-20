%%
%% Protocol macroses
%%
-ifndef(__PLAIN_PIPE_HRL__).
-define(__PLAIN_PIPE_HRL__, true).

-define(DATA(Data), {just, Data}).
-define(FINISH, nothing).

%% Producer protocol.
-define(DATA_MSG(Data), {self(), {just, Data}}).
-define(DATA_MSG(Producer, Data), {Producer, {just, Data}}).

-define(FINISH_MSG(), {self(), nothing}).
-define(FINISH_MSG(Producer), {Producer, nothing}).

%% Consumer protocol.
-define(ASK_MORE(), {self(), more}).
-define(ASK_MORE(Consumer), {Consumer, more}).

-endif.
