-module (key).
-export ([generate/0, between/3]).

% return a random number from 1 to 1.000.000.000
generate() ->
	random:uniform(1000000000).

%check if a Key is between From and To or equal to To
between(Key, From, To) ->
	if
		From < To ->
			((Key > From) and (Key =< To));
		From > To ->
			((Key > From) and (Key >= To)) or ((Key < From) and (Key =< To));
		From ==To ->
			true;
		true ->
			false
	end.
