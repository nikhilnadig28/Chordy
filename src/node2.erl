-module (node2).
-export([start/1, start/2, init/2, node/4, stabilize/3, stabilize/1]).
-define('Stabilize',500).
-define('Timeout',1000).

node(Id, Predecessor, Successor , Store) ->
	receive
		{key, Qref, Peer} ->
			Peer ! {Qref, Id},
			node(Id, Predecessor, Successor , Store);
		{notify, New } ->
			{Pred,NewStore} = notify(New, Id, Predecessor,Store),
			node(Id, Pred, Successor , NewStore);
		{request, Peer} ->
			request(Peer, Predecessor),
			node(Id, Predecessor, Successor , Store);
		{status, Pred } ->
			Succ = stabilize(Pred, Id, Successor),
			node(Id, Predecessor, Succ , Store);
		stabilize ->
			stabilize(Successor),
			node(Id, Predecessor, Successor , Store);
		probe ->
			create_probe(Id, Successor),
			node(Id, Predecessor, Successor , Store);
		{probe, Id, Nodes, T} ->
			remove_probe(T, Nodes),
			node(Id, Predecessor, Successor , Store);
		{probe, Ref, Nodes, T} ->
			forward_probe(Ref, T, Nodes, Id, Successor),
			node(Id, Predecessor, Successor , Store);
		status ->
			io:format("Node: ~w   Predecessor: ~w Successor: ~w  Storage: ~w~n", [Id, Predecessor, Successor, Store]),
			node(Id, Predecessor, Successor, Store);
		{add, Key, Value, Qref, Client} ->
			Added = add(Key, Value, Qref, Client, Id, Predecessor, Successor, Store),
			% io:format("Node Id : ~w~n",[Id]),
			node(Id, Predecessor, Successor, Added);
		{lookup, Key, Qref, Client} ->
			lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
			node(Id, Predecessor, Successor, Store);
		{handover, Elements} ->
			Merged = storage:merge(Store, Elements),
			node(Id, Predecessor, Successor, Merged);
		kill ->
			io:format("Node killed~n"),
			ok;
		Error ->
			io:format("Error! ~w~n", [Error]),
			node(Id,Predecessor,Successor , Store)
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

notify({Nkey, Npid}, Id, Predecessor , Store) ->
	case Predecessor of
		nil ->
			Keep = handover(Id, Store , Nkey , Npid),
			{{Nkey,Npid},Keep};
		{Pkey, _} ->
			case key:between(Nkey, Pkey, Id) of
				true ->
					Keep = handover(Id, Store , Nkey , Npid),
					{{Nkey,Npid},Keep};
				false ->
					{Predecessor,Store}
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
	Store = storage:create(),
	node(Id, Predecessor, Successor, Store).


connect(Id, nil) ->
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
	Pid ! {probe, Id,[Id],erlang:now()}.
%print Time
remove_probe(T, Nodes) ->
	Diff = timer:now_diff(erlang:now(), T),
	io:format("Removing probe after ~w microseconds. The probe has gone through ~w~n",[Diff, Nodes]).

%send prob to successor
forward_probe(Ref, T, Nodes, Id, Successor)->
	{_,Pid} = Successor,
	io:format("Forwarding probe at ~w ~n",[Id]),
	Pid ! {probe,Ref,Nodes ++ [Id],T}.


add(Key, Value, Qref, Client, Id, {Pkey, _}, {_, Spid}, Store) ->
		case key:between(Key, Pkey, Id) of
			true ->
				Client ! {Qref, ok},
				storage:add(Key, Value, Store);
			false ->
				Spid ! {add, Key, Value, Qref, Client},
				Store
		end.

lookup(Key, Qref, Client, Id, {Pkey, _}, Successor, Store)->
		case key:between(Key, Pkey, Id) of
			true ->
				Result = storage:lookup(Key, Store),
				Client ! {Qref, Result};
			false ->
				{_, Spid} = Successor,
				Spid ! {lookup, Key, Qref, Client} 
		end.


handover(Id, Store, Nkey, Npid) ->
		{Keep, Leave} = storage:split(Nkey, Id, Store),
		Npid ! {handover, Leave},
		Keep.