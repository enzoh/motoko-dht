import Bucket "../src/Bucket";
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

  private var bucket = Bucket.nil();
  private var identity = null : ?Key ;

  private let db = RBTree.RBTree<[Word8], [Word8]>(Util.compare);

  public func initialize() : async () {
    Log.info("Initializing...");
    if (Option.isSome(identity)) {
      Log.warn("Canister already initialized!");
    };
    let id = await whoami();
    Log.trace("id = " # id);
    let peer = Util.hexToKeyOrTrap(id);
    Log.trace("peer = " # Key.show(peer));
    bucket := Bucket.insert(peer, bucket);
    Log.trace("bucket = " # Bucket.show(bucket));
    identity := ?peer;
    Log.info("Ready...");
  };

  public shared {
    caller = caller;
  } func whoami() : async Id {
    Util.principalToHex(caller);
  };

  public func seed(id : Id) : async () {
    Log.info("Seeding...");
    if (Option.isNull(identity)) {
      Log.error("Canister not yet initialized!");
    };
    Log.trace("id = " # id);
    if (not Util.isId(id)) {
      Log.error("Invalid identifier!");
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
    // TODO: await migrate(peer);
  };

  public shared {
    caller = caller;
  } func ping() : async () {
    Log.info("Pong...");
    let id = Util.principalToHex(caller);
    Log.trace("id = " # id);
    if (not Util.isId(id)) {
      Log.error("Invalid identifier!");
    };
    let peer = Util.hexToKeyOrTrap(id);
    Log.trace("peer = " # Key.show(peer));
    bucket := Bucket.insert(peer, bucket);
    Log.trace("bucket = " # Bucket.show(bucket));
    // TODO: await migrate(peer);
  };

  public func get(key : [Word8]) : async ?[Word8] {
    await getWithTrace(key, List.nil<Key>());
  };

  public func getWithTrace(key : [Word8], route : Bucket) : async ?[Word8] {
    Log.info("Getting...");
    if (Option.isNull(identity)) {
      Log.error("Canister not yet initialized!");
    };
    let self = Option.unwrap<Key>(identity);
    let delta = Bucket.difference(bucket, route);
    Log.trace("delta = " # Bucket.show(delta));
    if (not List.isNil<Key>(delta)) {
      bucket := Bucket.union(bucket, route);
      Log.trace("bucket = " # Bucket.show(bucket));
      for (peer in List.toArray<Key>(delta).vals()) {
        // TODO: await migrate(peer);
      };
    };
    let to = Key.key(key);
    Log.trace("to = " # Key.show(to));
    let from = List.toArray<Key>(bucket);
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

  public func put(key : [Word8], value : [Word8]) : async () {
    await putWithTrace(key, value, List.nil<Key>());
  };

  public func putWithTrace(key : [Word8], value : [Word8], route : Bucket) : async () {
    Log.info("Putting...");
    if (Option.isNull(identity)) {
      Log.error("Canister not yet initialized!");
    };
    let self = Option.unwrap<Key>(identity);
    let delta = Bucket.difference(bucket, route);
    Log.trace("delta = " # Bucket.show(delta));
    if (not List.isNil<Key>(delta)) {
      bucket := Bucket.union(bucket, route);
      Log.trace("bucket = " # Bucket.show(bucket));
      for (peer in List.toArray<Key>(delta).vals()) {
        // TODO: await migrate(peer);
      };
    };
    let to = Key.key(key);
    Log.trace("to = " # Key.show(to));
    let from = List.toArray<Key>(bucket);
    Log.trace("from = " # Bucket.show(bucket));
    let closest = Key.sortByDistance(to, from)[0];
    Log.trace("closest = " # Key.show(closest));
    if (Key.equal(closest, self)) {
      let _ = db.insert(key, value);
    } else {
      let id = Util.keyToHex(closest);
      Log.trace("id = " # id);
      let shard = actor ("ic:" # id) : actor {
        putWithTrace(key : [Word8], value : [Word8], route : Bucket) : async ();
      };
      await shard.putWithTrace(key, value, Bucket.insert(self, route));
    };
  };
};
