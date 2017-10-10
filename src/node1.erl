-module (node1).
-export ([start/1, start/2]).
-define('Stabilize',500).
-define('Timeout',1000).

node(Id, Predecessor, Successor) ->
	receive
		{key, Qref, Peer} -> %  a peer needs to know our key
			Peer ! {Qref, Id},
			io:format("~w requested for ~w key.~n",[Peer, Id]),
			node(Id, Predecessor, Successor);
		{notify, New} -> %  a new node informs us of its existence
			Pred = notify(New, Id, Predecessor),
			% io:format("Predecessor of ~w is ~w~n",[Id, Pred]),
			node(Id, Pred, Successor);
		{request, Peer} -> % a predecessor needs to know our predecessor
			request(Peer, Predecessor),
			node(Id, Predecessor, Successor);
		{status, Pred} -> % our successor informs us about its predecessor
			Succ = stabilize(Pred, Id, Successor),
			% io:format("Successor of ~w is ~w~n",[Id, Succ]),
			node(Id, Predecessor, Succ);
		stabilize ->
			stabilize(Successor),
			node(Id, Predecessor, Successor);
		probe ->
			create_probe(Id, Successor),
			node(Id, Predecessor, Successor);
		{probe, Id, Nodes, T} ->
			remove_probe(T, Nodes),
			node(Id, Predecessor, Successor);
		{probe, Ref, Nodes, T} ->
			forward_probe(Ref, T, Nodes, Id, Successor),
			node(Id, Predecessor, Successor)
		
end.


stabilize(Pred, Id, Successor) ->
	{Skey, Spid} = Successor,
	case Pred of
		nil ->
			Spid ! {notify, {Id, self()}},
			Successor;
		{Id, _} ->
			Successor;
		{Skey, _} ->
			Spid ! {notify, {Id, self()}},
			Successor;
		{Xkey, Xpid} ->			
			case key:between(Xkey, Id, Skey) of
				true ->
					Xpid ! {notify, {Id, self()}},
					{Xkey, Xpid};
				false ->
					Spid ! {notify, {Id, self()}},
					Successor
			end
	end.

schedule_stabilize() ->
	timer:send_interval(?Stabilize, self(), stabilize).

stabilize({_, Spid}) ->
	Spid ! {request, self()}.

request(Peer, Predecessor) ->
	case Predecessor of
		nil ->
			Peer ! {status, nil};
		{Pkey, Ppid} ->
			Peer ! {status, {Pkey, Ppid}}
	end.

notify({Nkey, Npid}, Id, Predecessor) ->
	case Predecessor of
		nil ->
			Npid ! {status,{Nkey,Npid}},
			{Nkey,Npid};
		{Pkey, _} ->
			case key:between(Nkey, Pkey, Id) of
				true ->
					Npid ! {status,{Nkey,Npid}},
					{Nkey,Npid};
				false ->
					Npid ! {status,Predecessor},
					Predecessor
			end
	end.


start(Id) ->
	start(Id, nil).

start(Id, Peer) ->
	timer:start(),
	spawn(fun() -> init(Id, Peer) end).


init(Id, Peer) ->
	Predecessor = nil,
	{ok, Successor} = connect(Id, Peer),
	schedule_stabilize(),
	node(Id, Predecessor, Successor).


connect(Id, nil) ->
	io:format("Node ~w started.~n",[Id]),
	{ok, {Id,self()}};

connect(Id, Peer) ->
	Qref = make_ref(),
	Peer ! {key, Qref, self()},
	receive
		{Qref, Skey} ->
			io:format("Node ~w started.~n",[Id]),
			{ok,{Skey,Peer}}
	after 
		?Timeout ->
		io:format("Time out: no response~n")
	end.

%send first probe
create_probe(Id, Successor) ->
	{_,Pid} = Successor,
	io:format("Creating Probe~n"),
	Pid ! {probe, Id,[Id],erlang:system_time(micro_seconds)}.
%print Time
remove_probe(T, Nodes) ->
	Diff = (erlang:system_time(micro_seconds) - T),
	io:format("Removing probe after ~w microseconds. Probe has hooped through ~w~n",[Diff, Nodes]).

%send prob to successor
forward_probe(Ref, T, Nodes, Id, Successor)->
	{_,Pid} = Successor,
	io:format("Forwarding probe at ~w ~n",[Id]),
	Pid ! {probe,Ref,[Id|Nodes],T}.
