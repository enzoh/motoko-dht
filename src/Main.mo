import Array "mo:base/Array";
import AssocList "mo:base/AssocList";
import CRC8 "../vendor/crc/src/CRC8";
import Hex "../vendor/hex/src/Hex";
import Iter "mo:base/Iter";
import Key "Key";
import List "mo:base/List";
import Option "mo:base/Option";
import Prelude "mo:base/Prelude";
import Prim "mo:prim";
import Principal "mo:base/Principal";
import Util "Util";

actor {

  private type AssocList<K, V> = AssocList.AssocList<K, V>;
  private type ID = Text;
  private type Key = Key.Key;
  private type List<T> = List.List<T>;

  private var ready = false;
  private var self : ?ID = null;
  private var registry : List<Key> = null;
  private var db : AssocList<[Word8], [Word8]> = null;

  public func initialize() : async () {
    if (ready) {
      Prelude.printLn("WARN: Canister already initialized!");
    };
    let id = await whoami();
    Prelude.printLn("TRACE: id = " # id);
    await register(id);
    self := ?id;
    ready := true;
  };

  public shared {
    caller = caller;
  } func whoami() : async ID {
    let base = Iter.toArray<Word8>(Prim.blobOfPrincipal(caller).bytes());
    let crc8 = CRC8.crc8(base);
    Hex.encode(Array.append<Word8>(base, [crc8]));
  };

  public func register(id : ID) : async () {
    switch (Util.hexToKey(id)) {
      case (#ok key) {
        registry := List.push<Key>(key, registry);
      };
      case (#err (#msg str)) {
        Prelude.printLn("ERROR: " # str);
        Prelude.unreachable();
      };
    };
  };

  public func network() : async List<ID> {
    List.foldRight<Key, List<ID>>(registry, null, func (key, accum) {
      List.push<ID>(Hex.encode(key.preimage), accum);
    });
  };

  public func get(key : [Word8]) : async ?[Word8] {
    if (not ready) {
      Prelude.printLn("ERROR: Canister not yet initialized!");
      Prelude.unreachable();
    };
    let to = Key.key(key);
    let from = List.toArray<Key>(registry);
    let closest = Hex.encode(Key.sortByDistance(to, from)[0].preimage);
    if (closest == Option.unwrap<ID>(self)) {
      AssocList.find<[Word8], [Word8]>(db, key, eq);
    } else {
      let shard = actor ("ic:" # closest) : actor {
        get(key : [Word8]) : async ?[Word8];
      };
      await shard.get(key);
    };
  };

  public func put(key : [Word8], value : [Word8]) : async () {
    if (not ready) {
      Prelude.printLn("ERROR: Canister not yet initialized!");
      Prelude.unreachable();
    };
    let to = Key.key(key);
    let from = List.toArray<Key>(registry);
    let closest = Hex.encode(Key.sortByDistance(to, from)[0].preimage);
    if (closest == Option.unwrap<ID>(self)) {
      db := AssocList.replace<[Word8], [Word8]>(db, key, eq, ?value).0;
    } else {
      let shard = actor ("ic:" # closest) : actor {
        put(key : [Word8], value : [Word8]) : async ();
      };
      await shard.put(key, value);
    };
  };

  private func eq(a : [Word8], b : [Word8]) : Bool {
    Array.equals<Word8>(a, b, func (ai, bi) {
      return ai == bi;
    })
  };
};
