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

-module(gen_zmq_socket_sub).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("gen_zmq_internal.hrl").

-export([init/1, close/4, encap_msg/4, decap_msg/5]).
-export([idle/4]).

-record(state, {
}).

%%%===================================================================
%%% API
%%%===================================================================

%%%===================================================================
%%% gen_zmq_socket callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the Fsm
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------

init(_Opts) ->
	{ok, idle, #state{}}.

close(_StateName, _Transport, MqSState, State) ->
	{next_state, idle, MqSState, State}.

encap_msg({_Transport, Msg}, _StateName, _MqSState, _State) ->
	gen_zmq:simple_encap_msg(Msg).
decap_msg(_Transport, {_RemoteId, Msg}, _StateName, _MqSState, _State) ->
	gen_zmq:simple_decap_msg(Msg).

idle(check, deliver, _MqSState, _State) ->
	ok;
idle(check, {deliver_recv, _Transport}, _MqSState, _State) ->
	ok;
idle(check, recv, _MqSState, _State) ->
	ok;
idle(check, _, _MqSState, _State) ->
	{error, fsm};

idle(do, {deliver, _Transport}, MqSState, State) ->
	{next_state, idle, MqSState, State};
idle(do, {queue, _Transport}, MqSState, State) ->
	{next_state, idle, MqSState, State};
idle(do, {dequeue, _Transport}, MqSState, State) ->
	{next_state, idle, MqSState, State};
idle(do, _, _MqSState, _State) ->
	{error, fsm}.
