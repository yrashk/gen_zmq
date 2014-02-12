-module(integration).
-include_lib("proper/include/proper.hrl").
-compile(export_all).

prop_echo(IP) ->
    {ok, Socket} = gen_zmq:start([{type, dealer}]),
    gen_zmq:connect(Socket, tcp, IP, 5555, []),
    ?FORALL(A,
	    term(),
	    begin
		gen_zmq:send(Socket, [term_to_binary(A)]),
		{ok, [R]} = gen_zmq:recv(Socket),
		Rb = binary_to_term(R),
		Rb =:= A
	    end).

