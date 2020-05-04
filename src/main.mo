import AssocList "mo:stdlib/assocList";
import Hex "../vendor/hex/src/hex";
import Key "key";
import List "mo:stdlib/list";
import Prelude "mo:stdlib/prelude";
import Util "util";

actor {

  private type Key = Key.Key;
  private type List<T> = List.List<T>;
  private type ShardId = Text;

  private var registry = List.nil<Key>();

  public query func shards() : async List<ShardId> {
    List.foldRight<Key, List<ShardId>>(
      registry,
      List.nil<ShardId>(),
      func (key, accum) {
        List.push<ShardId>(Hex.encode(key.preimage), accum);
      },
    );
  };

  public func register(shardId : ShardId) {
    switch (Util.hexToKey(shardId)) {
      case (#ok key) {
        registry := List.push<Key>(key, registry);
      };
      case (#err (#msg str)) {
        Prelude.printLn("ERROR: " # str);
        Prelude.unreachable();
      };
    };
  };

  private var db = List.nil<(Key, [Word8])>();

  public query func getLocal(key : Key) : async ?[Word8] {
    AssocList.find<Key, [Word8]>(db, key, Key.equal);
  };

  public func putLocal(key : Key, value : [Word8]) {
    db := AssocList.replace<Key, [Word8]>(db, key, Key.equal, ?value).0;
  };

  public func get(key : [Word8]) : async ?[Word8] {
    if (List.isNil(registry)) {
      Prelude.printLn("ERROR: Registry is empty!");
      Prelude.unreachable();
    };
    let to = Key.key(key);
    let from = List.toArray<Key>(registry);
    let shardId = Hex.encode(Key.sortByDistance(to, from)[0].preimage);
    let shard = actor ("ic:" # shardId) : actor {
      getLocal(key : Key) : async ?[Word8]
    };
    await shard.getLocal(to);
  };

  public func put(key : [Word8], value : [Word8]) {
    if (List.isNil(registry)) {
      Prelude.printLn("ERROR: Registry is empty!");
      Prelude.unreachable();
    };
    let to = Key.key(key);
    let from = List.toArray<Key>(registry);
    let shardId = Hex.encode(Key.sortByDistance(to, from)[0].preimage);
    let shard = actor ("ic:" # shardId) : actor {
      putLocal(key : Key, value : [Word8]) : async ()
    };
    await shard.putLocal(to, value);
  };
};
