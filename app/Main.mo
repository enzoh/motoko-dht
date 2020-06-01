import Bucket "../src/Bucket";
import Hex "../vendor/hex/src/Hex";
import Key "../src/Key";
import List "mo:base/List";
import Log "../src/Log";
import Option "mo:base/Option";
import RBTree "../tmp/RBTree";
import Util "../src/Util";

actor {

  private type Bucket = Bucket.Bucket;
  private type Id = Text;
  private type Key = Key.Key;

  private var axis = null : ?Key ;
  private var bucket = Bucket.nil();
  private var rebalance = false;

  private let db = RBTree.RBTree<[Word8], [Word8]>(Util.compare);

  public func initialize() : async () {
    Log.info("Initializing...");
    if (Option.isSome(axis)) {
      Log.warn("Canister already initialized!");
    };
    let id = await whoami();
    Log.info("I am ic:" # id);
    let self = Util.hexToKeyOrTrap(id);
    axis := ?self;
    bucket := Bucket.insert(self, bucket);
    Log.trace("bucket = " # Bucket.show(bucket));
  };

  public shared {
    caller = caller;
  } func whoami() : async Id {
    Util.principalToHex(caller);
  };

  public func configure(id : Id) : async () {
    Log.info("Configuring...");
    if (not Util.isId(id)) {
      Log.error("Invalid identifier: " # id);
    };
    let shard = actor ("ic:" # id) : actor {
      ping() : async ();
    };
    Log.info("Ping...");
    try {
      await shard.ping();
    } catch _ {
      Log.error("Canister unreachable!");
    };
    let peer = Util.hexToKeyOrTrap(id);
    Log.trace("peer = " # Key.show(peer));
    bucket := Bucket.insert(peer, bucket);
    Log.trace("bucket = " # Bucket.show(bucket));
    rebalance := true;
    Log.trace("rebalance = true");
  };

  public shared {
    caller = caller;
  } func ping() : async () {
    Log.info("Pong...");
    let id = Util.principalToHex(caller);
    Log.trace("id = " # id);
    if (not Util.isId(id)) {
      Log.error("Invalid identifier: " # id);
    };
    let peer = Util.hexToKeyOrTrap(id);
    Log.trace("peer = " # Key.show(peer));
    bucket := Bucket.insert(peer, bucket);
    Log.trace("bucket = " # Bucket.show(bucket));
    rebalance := true;
    Log.trace("rebalance = true");
  };

  public func get(key : [Word8]) : async ?[Word8] {
    await getWithTrace(key, List.nil());
  };

  public func put(key : [Word8], value : [Word8]) : async Bool {
    await putWithTrace(key, value, List.nil());
  };

  public func getWithTrace(key : [Word8], route : Bucket) : async ?[Word8] {
    Log.info("Getting...");
    if (Option.isNull(axis)) {
      await initialize();
    };
    let self = Option.unwrap(axis);
    let diff = Bucket.difference(bucket, route);
    Log.trace("diff = " # Bucket.show(diff));
    if (not List.isNil(diff)) {
      bucket := Bucket.union(bucket, route);
      Log.trace("bucket = " # Bucket.show(bucket));
      rebalance := true;
      Log.trace("rebalance = true");
    };
    let to = Key.key(key);
    Log.trace("to = " # Key.show(to));
    let from = List.toArray(bucket);
    Log.trace("from = " # Bucket.show(bucket));
    let closest = Key.sortByDistance(to, from)[0];
    Log.trace("closest = " # Key.show(closest));
    if (Key.equal(closest, self)) {
      db.find(key);
    } else {
      let id = Util.keyToHex(closest);
      Log.trace("id = " # id);
      let shard = actor ("ic:" # id) : actor {
        getWithTrace(key : [Word8], route : Bucket) : async ?[Word8];
      };
      await shard.getWithTrace(key, Bucket.insert(self, route));
    };
  };

  public func putWithTrace(key : [Word8], value : [Word8], route : Bucket) : async Bool {
    Log.info("Putting...");
    if (Option.isNull(axis)) {
      await initialize();
    };
    let self = Option.unwrap(axis);
    let diff = Bucket.difference(bucket, route);
    Log.trace("diff = " # Bucket.show(diff));
    if (not List.isNil(diff)) {
      bucket := Bucket.union(bucket, route);
      Log.trace("bucket = " # Bucket.show(bucket));
      rebalance := true;
      Log.trace("rebalance = true");
    };
    let to = Key.key(key);
    Log.trace("to = " # Key.show(to));
    let from = List.toArray(bucket);
    Log.trace("from = " # Bucket.show(bucket));
    let closest = Key.sortByDistance(to, from)[0];
    Log.trace("closest = " # Key.show(closest));
    if (Key.equal(closest, self)) {
      let _ = db.insert(key, value);
      true;
    } else {
      let id = Util.keyToHex(closest);
      Log.trace("id = " # id);
      let shard = actor ("ic:" # id) : actor {
        putWithTrace(key : [Word8], value : [Word8], route : Bucket) : async Bool;
      };
      await shard.putWithTrace(key, value, Bucket.insert(self, route));
    };
  };

  /**
   * Same as `get`, but with hex-encoded arguments. Traps for invalid
   * arguments. Useful for debugging and testing.
   */
  public func getInHex(keyInHex : Text) : async ?Text {
    switch (Hex.decode(keyInHex)) {
      case (#ok key) {
        Option.map(Hex.encode, await get(key));
      };
      case (#err (#msg str)) {
        Log.error("Invalid key: " # str);
        null;
      };
    };
  };

  /**
   * Same as `put`, but with hex-encoded arguments. Traps for invalid
   * arguments. Useful for debugging and testing.
   */
  public func putInHex(keyInHex : Text, valueInHex : Text) : async Bool {
    switch (Hex.decode(keyInHex), Hex.decode(valueInHex)) {
      case (#ok key, #ok value) {
        await put(key, value);
      };
      case (#err (#msg str), _) {
        Log.error("Invalid key: " # str);
        false;
      };
      case (_, #err (#msg str)) {
        Log.error("Invalid value: " # str);
        false;
      };
    };
  };

  public func size() : async Nat {
    RBTree.size(db.getTree())
  };
};
