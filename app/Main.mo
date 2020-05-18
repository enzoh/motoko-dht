import Array "mo:base/Array";
import CRC8 "../vendor/crc/src/CRC8";
import Hex "../vendor/hex/src/Hex";
import Iter "mo:base/Iter";
import Key "../src/Key";
import List "mo:base/List";
import Log "../src/Log";
import Option "mo:base/Option";
import Prim "mo:prim";
import RBTree "../tmp/RBTree";
import Util "../src/Util";

actor {

  private type Id = Text;
  private type Key = Key.Key;
  private type List<T> = List.List<T>;

  private var ready = false;
  private var registry : List<Key> = null;
  private var self : ?Id = null;

  private let db = RBTree.RBTree<[Word8], [Word8]>(Util.compare);

  public func initialize() : async () {
    Log.info("Initializing...");
    if (ready) {
      Log.warn("Canister already initialized!");
    };
    let id = await identity();
    Log.trace("id = " # id);
    await register(id);
    self := ?id;
    ready := true;
  };

  public shared {
    caller = caller;
  } func identity() : async Id {
    let base = Iter.toArray<Word8>(Prim.blobOfPrincipal(caller).bytes());
    let crc8 = CRC8.crc8(base);
    Hex.encode(Array.append<Word8>(base, [crc8]));
  };

  public func register(id : Id) : async () {
    Log.info("Registering...");
    Log.trace("id = " # id);
    if (not Util.isId(id)) {
      Log.fatal("Invalid identifier!");
    };
    switch (Util.hexToKey(id)) {
      case (#ok key) {
        registry := List.push<Key>(key, registry);
      };
      case (#err (#msg str)) {
        Log.fatal(str);
      };
    };
  };

  public func network() : async List<Id> {
    List.foldRight<Key, List<Id>>(registry, null, func (key, accum) {
      List.push<Id>(Hex.encode(key.preimage), accum);
    });
  };

  public func get(key : [Word8]) : async ?[Word8] {
    Log.info("Getting...");
    if (not ready) {
      Log.fatal("Canister not yet initialized!");
    };
    let to = Key.key(key);
    let from = List.toArray<Key>(registry);
    let closest = Hex.encode(Key.sortByDistance(to, from)[0].preimage);
    Log.trace("closest = " # closest);
    if (closest == Option.unwrap<Id>(self)) {
      db.find(key);
    } else {
      let shard = actor ("ic:" # closest) : actor {
        get(key : [Word8]) : async ?[Word8];
      };
      await shard.get(key);
    };
  };

  public func put(key : [Word8], value : [Word8]) : async () {
    Log.info("Putting...");
    if (not ready) {
      Log.fatal("Canister not yet initialized!");
    };
    let to = Key.key(key);
    let from = List.toArray<Key>(registry);
    let closest = Hex.encode(Key.sortByDistance(to, from)[0].preimage);
    Log.trace("closest = " # closest);
    if (closest == Option.unwrap<Id>(self)) {
      let _ = db.insert(key, value);
    } else {
      let shard = actor ("ic:" # closest) : actor {
        put(key : [Word8], value : [Word8]) : async ();
      };
      await shard.put(key, value);
    };
  };
};
