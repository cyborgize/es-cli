
open Devkit

module J = Yojson.Safe

module Version = struct

  type exact = [
    | `ES5
    | `ES6
    | `ES7
    | `ES8
  ]

  type t = [
    | exact
    | `Auto
  ]

  let wrap = function
    | `String "auto" -> `Auto
    | `Int 5 | `String "5" | `Intlit "5" -> `ES5
    | `Int 6 | `String "6" | `Intlit "6" -> `ES6
    | `Int 7 | `String "7" | `Intlit "7" -> `ES7
    | `Int 8 | `String "8" | `Intlit "8" -> `ES8
    | x -> Exn.fail "unknown ES version %s" (J.to_string x)

  let unwrap = function
    | `Auto -> `String "auto"
    | `ES5 -> `Int 5
    | `ES6 -> `Int 6
    | `ES7 -> `Int 7
    | `ES8 -> `Int 8

end
