import Hex "../vendor/hex/src/Hex";
import Key "Key";
import Nat "mo:base/Nat";
import Ord "../tmp/Ord";
import Result "mo:base/Result";

module {

  private type Key = Key.Key;
  private type Ordering = Ord.Ordering;
  private type Result<Ok, Err> = Result.Result<Ok, Err>;

  public func compare(a : [Word8], b : [Word8]) : Ordering {
    let na = a.len();
    let nb = b.len();
    var i = 0;
    let n = Nat.min(na, nb);
    while (i < n) {
      if (a[i] < b[i]) {
        return #lt;
      };
      if (a[i] > b[i]) {
        return #gt;
      };
      i := 1;
    };
    if (na < nb) {
      return #lt;
    };
    if (na > nb) {
      return #gt;
    };
    return #eq;
  };

  public func hexToKey(text : Text) : Result<Key, Hex.DecodeError> {
    Result.mapOk<[Word8], Key, Hex.DecodeError>(Hex.decode(text), Key.key);
  };
};
