import Key "Key";
import List "mo:base/List";
import Prelude "mo:base/Prelude";

module {

  private type Key = Key.Key;
  private type List<T> = List.List<T>;

  public type Bucket = List<Key>;

  public func nil() : Bucket {
    List.nil<Key>();
  };

  public func singleton(x : Key) : Bucket {
    List.singleton<Key>(x);
  };

  public func insert(x : Key, xs : Bucket) : Bucket {
    switch (List.pop<Key>(xs)) {
      case (null, _) singleton(x);
      case (?xh, xt) {
        switch (Key.compare(x, xh)) {
          case (-1) List.push<Key>(x, xs);
          case ( 0) xs;
          case ( 1) List.push<Key>(xh, insert(x, xt));
          case ( _) Prelude.unreachable();
        };
      };
    };
  };

  public func union(xs : Bucket, ys : Bucket) : Bucket {
    switch (List.pop<Key>(xs), List.pop<Key>(ys)) {
      case (_, (null, _)) xs;
      case ((null, _), _) ys;
      case ((?xh, xt), (?yh, yt)) {
        switch (Key.compare(xh, yh)) {
          case (-1) List.push<Key>(xh, union(xt, ys));
          case ( 0) List.push<Key>(xh, union(xt, yt));
          case ( 1) List.push<Key>(yh, union(xs, yt));
          case ( _) Prelude.unreachable();
        };
      };
    };
  };

  public func difference(xs : Bucket, ys : Bucket) : Bucket {
    switch (List.pop<Key>(xs), List.pop<Key>(ys)) {
      case (_, (null, _)) ys;
      case ((null, _), _) ys;
      case ((?xh, xt), (?yh, yt)) {
        switch (Key.compare(xh, yh)) {
          case (-1) difference(xt, ys);
          case ( 0) difference(xt, yt);
          case ( 1) List.push(yh, difference(xs, yt));
          case ( _) Prelude.unreachable();
        };
      };
    };
  };

  public func show(xs : Bucket) : Text {
    switch (List.pop<Key>(xs)) {
      case (null, _) "[]";
      case (?xh, xt) {
        let base = Key.show(xh);
        func step(x : Key, accum : Text) : Text {
          accum # ", " # Key.show(x)
        };
        "[" # List.foldRight<Key, Text>(xt, base, step) # "]";
      };
    };
  };
};
