import Prelude "mo:base/Prelude";

module {

  public func trace(message : Text) {
    Prelude.printLn("TRACE: " # message);
  };

  public func info(message : Text) {
    Prelude.printLn("INFO: " # message);
  };

  public func warn(message : Text) {
    Prelude.printLn("WARN: " # message);
  };

  public func error(message : Text) {
    Prelude.printLn("ERROR: " # message);
  };

  public func fatal(message : Text) {
    Prelude.printLn("FATAL: " # message);
    Prelude.unreachable();
  };
};
