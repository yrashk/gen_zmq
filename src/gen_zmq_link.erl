% Copyright 2010-2011, Travelping GmbH <info@travelping.com>

% Permission is hereby granted, free of charge, to any person obtaining a
% copy of this software and associated documentation files (the "Software"),
% to deal in the Software without restriction, including without limitation
% the rights to use, copy, modify, merge, publish, distribute, sublicense,
% and/or sell copies of the Software, and to permit persons to whom the
% Software is furnished to do so, subject to the following conditions:

% The above copyright notice and this permission notice shall be included in
% all copies or substantial portions of the Software.

% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
% DEALINGS IN THE SOFTWARE.

-module(gen_zmq_link).

-behaviour(gen_fsm).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("gen_zmq_debug.hrl").

%% API
-export([start_link/0]).
-export([start_connection/0, accept/4, connect/6, connect/7, close/1]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
		 handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).
-export([setup/2, open/2, connecting/2, connected/2, send/2]).

-define(SERVER, ?MODULE).

-record(state, {
		  mqsocket                  :: pid(),
		  identity = <<>>           :: binary(),
		  remote_id = <<>>          :: binary(),
		  socket,
		  version = 0,
		  frames = [],
		  pending = <<>>
		 }).


-define(STARTUP_TIMEOUT, 10000).     %% wait 10sec for someone to tell us what to do
-define(CONNECT_TIMEOUT, 10000).     %% wait 10sec for the first packet to arrive
-define(REQUEST_TIMEOUT, 10000).     %% wait 10sec for answer
-define(TCP_OPTS, [binary, inet6,
                   {active,       false},
				   {send_timeout, 5000},
                   {backlog,      100},
                   {nodelay,      true},
                   {packet,       raw},
                   {reuseaddr,    true}]).

-ifdef(debug).
-define(FSM_OPTS,{debug,[trace]}).
-else.
-define(FSM_OPTS,).
-endif.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_fsm:start_link(?MODULE, [], [?FSM_OPTS]).

start_connection() ->
	gen_zmq_link_sup:start_connection().

accept(MqSocket, Identity, Server, Socket) ->
    gen_tcp:controlling_process(Socket, Server),
	gen_fsm:send_event(Server, {accept, MqSocket, Identity, Socket}).

connect(Identity, Server, unix, Path, TcpOpts, Timeout) ->
	gen_fsm:send_event(Server, {connect, self(), Identity, unix, Path, TcpOpts, Timeout}).

connect(Identity, Server, tcp, Address, Port, TcpOpts, Timeout) ->
	gen_fsm:send_event(Server, {connect, self(), Identity, tcp, Address, Port, TcpOpts, Timeout}).

send(Server, Msg) ->
	gen_fsm:send_event(Server, {send, Msg}).

close(Server) ->
	gen_fsm:sync_send_all_state_event(Server, close).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    {ok, setup, #state{}, ?STARTUP_TIMEOUT}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
setup({accept, MqSocket, Identity, Socket}, State) ->
	?DEBUG("got setup~n"),
	NewState = State#state{mqsocket = MqSocket, identity = Identity, socket = Socket},
	?DEBUG("NewState: ~p~n", [NewState]),
	send_frames([Identity], {next_state, open, NewState, ?CONNECT_TIMEOUT});

setup({connect, MqSocket, Identity, tcp, Address, Port, TcpOpts, Timeout}, State) ->
	?DEBUG("got connect: ~w, ~w~n", [Address, Port]),

	%%TODO: socket options
	case gen_tcp:connect(Address, Port, TcpOpts, Timeout) of
		{ok, Socket} ->
			NewState = State#state{mqsocket = MqSocket, identity = Identity, socket = Socket},
			ok = inet:setopts(Socket, [{active, once}]),
			{next_state, connecting, NewState, ?CONNECT_TIMEOUT};
		Reply ->
			gen_zmq:deliver_connect(MqSocket, Reply),
			{stop, normal, State}				
	end;

setup({connect, MqSocket, Identity, unix, Path, TcpOpts, _Timeout}, State) ->
	?DEBUG("got unix connect: ~p~n", [Path]),

	%%TODO: socket options
    {ok, Fd} = gen_socket:socket(unix, stream, 0),
    case gen_socket:connect(Fd, gen_socket:sockaddr_unix(Path)) of
		ok -> case gen_tcp:fdopen(Fd, TcpOpts) of
				  {ok, Socket} ->
					  NewState = State#state{mqsocket = MqSocket, identity = Identity, socket = Socket},
					  ok = inet:setopts(Socket, [{active, once}]),
					  ?DEBUG("unix connect ok~n"),
					  {next_state, connecting, NewState, ?CONNECT_TIMEOUT};
				  Reply ->
					  gen_zmq:deliver_connect(MqSocket, Reply),
					  ?DEBUG("unix connect fail ~p,~p~n", [Reply, TcpOpts]),
					  {stop, normal, State}				
			  end;
		Reply ->
			gen_zmq:deliver_connect(MqSocket, Reply),
			{stop, normal, State}				
	end.

connecting(timeout, State = #state{mqsocket = MqSocket}) ->
	?DEBUG("timeout in connecting~n"),
	gen_zmq:deliver_connect(MqSocket, {error, timeout}),
	{stop, normal, State};

connecting({in, Frames}, State = #state{mqsocket = MqSocket, identity = Identity})
  when length(Frames) == 1 ->
	?DEBUG("Frames in connecting: ~p~n", [Frames]),
	[RemoteId0] = gen_zmq:simple_decap_msg(Frames),
	RemoteId = gen_zmq:remote_id_assign(RemoteId0),
	gen_zmq:deliver_connect(MqSocket, {ok, RemoteId}),
	send_frames([Identity], {next_state, connected, State #state{remote_id = RemoteId}});

connecting({in, Frames}, State = #state{mqsocket = MqSocket}) ->
	?DEBUG("Invalid frames in connecting: ~p~n", [Frames]),
	gen_zmq:deliver_connect(MqSocket, {error, data}),
	{stop, normal, State}.

open(timeout, State) ->
	?DEBUG("timeout in open~n"),
	{stop, normal, State};

open({in, Frames}, #state{mqsocket = MqSocket} = State)
  when length(Frames) == 1 ->
	?DEBUG("Frames in open: ~p~n", [Frames]),
	[RemoteId0] = gen_zmq:simple_decap_msg(Frames),
	RemoteId = gen_zmq:remote_id_assign(RemoteId0),
	gen_zmq:deliver_accept(MqSocket, RemoteId),
	{next_state, connected, State#state{remote_id = RemoteId}};

open({in, Frames}, State) ->
	?DEBUG("Invalid frames in open: ~p~n", [Frames]),
	{stop, normal, State}.

connected(timeout, State) ->
	?DEBUG("timeout in connected~n"),
	{stop, normal, State};

connected({in, [Head|Frames]}, #state{mqsocket = MqSocket, remote_id = RemoteId} = State) ->
	?DEBUG("in connected Head: ~w, Frames: ~p~n", [Head, Frames]),
	gen_zmq:deliver_recv(MqSocket, {RemoteId, Frames}),
	{next_state, connected, State};

connected({send, Msg}, State) ->
	send_frames([<<>>|Msg], {next_state, connected, State}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------



%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
	{next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(close, _From, _StateName, State) ->
	{stop, normal, ok, State};

handle_sync_event(_Event, _From, StateName, State) ->
	Reply = ok,
	{reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info({'EXIT', MqSocket, _Reason}, _StateName, #state{mqsocket = MqSocket} = State) ->
	{stop, normal, State#state{mqsocket = undefined}};

handle_info({tcp, Socket, Data}, StateName, #state{socket = Socket} = State) ->
	?DEBUG("handle_info: ~p~n", [Data]),
	State1 = State#state{pending = <<(State#state.pending)/binary, Data/binary>>},
	handle_data(StateName, State1, {next_state, StateName, State1});

handle_info({tcp_closed, Socket}, _StateName, #state{socket = Socket} = State) ->
	?DEBUG("client disconnected: ~w~n", [Socket]),
	{stop, normal, State}.

handle_data(_StateName, #state{socket = Socket, pending = <<>>}, ProcessStateNext) ->
	ok = inet:setopts(Socket, [{active, once}]),
	ProcessStateNext;

handle_data(StateName, #state{socket = Socket, version = Ver, pending = Pending} = State, ProcessStateNext) ->
	{Msg, DataRest} = gen_zmq_frame:decode(Ver, Pending),
	State1 = State#state{pending = DataRest},
	?DEBUG("handle_info: decoded: ~p~nrest: ~p~n", [Msg, DataRest]),

	case Msg of
		more ->
			ok = inet:setopts(Socket, [{active, once}]),
			setelement(3, ProcessStateNext, State1);

		{true, Frame} ->
			State2 = State1#state{frames = [Frame|State1#state.frames]},
			handle_data(StateName, State2, setelement(3, ProcessStateNext, State2));

		{false, Frame} ->
			Frames = lists:reverse([Frame|State1#state.frames]),
			State2 = State1#state{frames = []},
			?DEBUG("handle_data: finale decoded: ~p~n", [Frames]),
			Reply = exec_sync(Frames, StateName, State2),
			?DEBUG("handle_data: reply: ~p~n", [Reply]),
			case element(1, Reply) of
				next_state ->
					handle_data(element(2, Reply), element(3, Reply), Reply);
				_ ->
					Reply
			end
	end.

	
%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, #state{mqsocket = MqSocket, socket = Socket})
  when is_port(Socket) ->
	?DEBUG("terminate"),
	catch gen_zmq:deliver_close(MqSocket),
	gen_tcp:close(Socket),
	ok;
terminate(_Reason, _StateName, #state{mqsocket = MqSocket}) ->
	?DEBUG("terminate"),
	catch gen_zmq:deliver_close(MqSocket),
	ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
	{ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

exec_sync(Msg, StateName, State) ->
	?MODULE:StateName({in, Msg}, State).

send_frames(Frames, NextStateInfo) ->
	State = element(3, NextStateInfo),
	Socket = State#state.socket,

	Packet = gen_zmq_frame:encode(Frames),
	case gen_tcp:send(Socket, Packet) of
		ok ->
			ok = inet:setopts(Socket, [{active, once}]),
			NextStateInfo;
		{error, Reason} ->
			?DEBUG("error - Reason: ~p~n", [Reason]),
			{stop, Reason, State}
	end.
