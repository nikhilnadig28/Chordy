-module (node3).
-export([start/1, start/2, init/2, node/5, stabilize/4, stabilize/1]).
-define('Stabilize',500).
-define('Timeout',1000).

node(Id, Predecessor, Successor , Store, Next) ->
	receive
		{key, Qref, Peer} ->
			Peer ! {Qref, Id},
			node(Id, Predecessor, Successor , Store, Next);
		{notify, New } ->
			{Pred,NewStore} = notify(New, Id, Predecessor, Store),
			node(Id, Pred, Successor , NewStore, Next);
		{request, Peer} ->
			request(Peer, Predecessor, Successor),
			node(Id, Predecessor, Successor , Store, Next);
		{status, Pred, Nx} ->
			{Succ, Nxt} = stabilize(Pred, Id, Successor, Nx),
			node(Id, Predecessor, Succ, Store, Nxt);
		stabilize ->
			stabilize(Successor),
			node(Id, Predecessor, Successor , Store, Next);
		probe ->
			create_probe(Id, Successor),
			node(Id, Predecessor, Successor , Store, Next);
		{probe, Id, Nodes, T} ->
			remove_probe(T, Nodes),
			node(Id, Predecessor, Successor , Store, Next);
		{probe, Ref, Nodes, T} ->
			forward_probe(Ref, T, Nodes, Id, Successor),
			node(Id, Predecessor, Successor , Store, Next);
		status ->
			io:format("Node: ~w   Predecessor: ~w Successor: ~w Next: ~w  Storage: ~w~n", [Id, Predecessor, Successor, Next, Store]),
			node(Id, Predecessor, Successor, Store, Next);
		kill ->
			io:format("Node killed"),
			ok;
		{add, Key, Value, Qref, Client} ->
			Added = add(Key, Value, Qref, Client, Id, Predecessor, Successor, Store),
			node(Id, Predecessor, Successor, Added, Next);
		{lookup, Key, Qref, Client} ->
			lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
			node(Id, Predecessor, Successor, Store, Next);
		{handover, Elements} ->
			Merged = storage:merge(Store, Elements),
			node(Id, Predecessor, Successor, Merged, Next);
		{'DOWN', Ref, process, _, _} ->
			{Pred, Succ, Nxt} = down(Ref, Predecessor, Successor, Next),
			node(Id, Pred, Succ, Store, Nxt);
		Error ->
			io:format("Error! ~w~n", [Error]),
			node(Id,Predecessor,Successor , Store, Next)
	end.



stabilize(Pred, Id, Successor, Nx) ->
	{Skey, SRef, Spid} = Successor,
	case Pred of
		nil ->
			Spid ! {notify, {Id, self()}},
			{Successor,Nx};
		{Id, _} ->
			{Successor,Nx};
		{Skey, _} ->
			Spid ! {notify, {Id, self()}},
			{Successor,Nx};
		{Xkey, Xpid} ->			
			case key:between(Xkey, Id, Skey) of
				true ->
					Xpid ! {notify, {Id, self()}},
					drop(SRef),
					Xref = monitor(Xpid),
					self() ! stabilize,
					{{Xkey,Xref, Xpid}, Successor};
				false ->
					Spid ! {notify, {Id, self()}},
					{Successor,Nx}
			end
	end.


schedule_stabilize() ->
	timer:send_interval(?Stabilize, self(), stabilize).

stabilize({_, _, Spid}) ->
	Spid ! {request, self()}.

request(Peer, Predecessor, Next) ->
	case Predecessor of
		nil ->
			Peer ! {status, nil, Next};
		{Pkey, _ , Ppid} ->
			Peer ! {status, {Pkey, Ppid}, Next}
	end.

notify({Nkey, Npid}, Id, Predecessor, Store) ->
    case Predecessor of
        nil ->
            Keep = handover(Id, Store, Nkey, Npid), 
            Nref = monitor(Npid),
            {{Nkey, Nref, Npid}, Keep};
        {Pkey,Pref, Pid} ->
            case key:between(Nkey, Pkey, Id) of
                true ->
                    Keep = handover(Id, Store, Nkey, Npid),
                    drop(Pref),
                    NRef = monitor(Npid),
                    {{Nkey, NRef , Npid}, Keep};
                false ->
                    {Predecessor, Store}
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
	Next = nil,
	node(Id, Predecessor, Successor, Store, Next).


connect(Id, nil) ->
	{ok, {Id, nil, self()}};

connect(Id, Peer) ->
	Qref = make_ref(),
	Peer ! {key, Qref, self()},
	receive
		{Qref, Skey} ->
			Ref = monitor(Peer),
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
				io:format("Adding Key ~w and Value ~w~n",[Key, Value]),
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

monitor(Pid) ->
	erlang:monitor(process, Pid).

drop(nil) ->
	ok;

drop(Pid) ->
	erlang:demonitor(Pid, [flush]).

down(Ref, {_, Ref, _}, Successor, Next) ->
	io:format("Predecessor died. ~n"),
	{nil, Successor, Next};

down(Ref, Predecessor, {_, Ref, _}, {Nkey,_, Npid}) ->	
	self() ! stabilize,
	Nref = monitor(Npid),
	io:format("Successor died.~n"),
	{Predecessor, {Nkey, Nref, Npid}, nil}.