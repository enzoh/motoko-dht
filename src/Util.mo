import Hex "../vendor/hex/src/Hex";
import Key "Key";
import Result "mo:base/Result";

module {

  private type Key = Key.Key;
  private type Result<Ok, Err> = Result.Result<Ok, Err>;

  public func hexToKey(text : Text) : Result<Key, Hex.DecodeError> {
    Result.mapOk<[Word8], Key, Hex.DecodeError>(Hex.decode(text), Key.key);
  };
};