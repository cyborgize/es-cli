
let () =
  let open Mybuild in
  Version.save "version.ml";
  Atdgen.setup ();
  ()
