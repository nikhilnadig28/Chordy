-module (storage).
-export ([create/0, add/3, lookup/2, split/3, merge/2]).

create() -> % create a new store
 	[].

add(Key, Value, Store) -> % add a key value pair, return the updated store
	List = lists:append(Store, [{Key, Value}]),
	lists:keysort(1, List).

lookup(Key, Store) -> % return a tuple {Key, Value} or the atom false
	lists:keyfind(Key, 1, Store).

split(From, To, Store) -> % return a tuple {Updated, Rest} where the updated store only contains the key value pairs requested and the rest are found in a list of key-value pairs
	lists:partition(fun({Key, _})-> key:between(Key, From, To) end, Store).

merge(Store1, Store2) -> % add a list of key-value pairs to a store
	lists:append(Store1, Store2).