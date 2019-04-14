open Devkit
open ExtLib
open Printf

module J = Yojson.Safe

let log = Log.from "common"

let version = sprintf "%s [%s@%s]" Version.id Version.user Version.host

type config = {
  host : string;
  nodes : string list option;
  version : Config_t.version option;
}

let expand_node =
  let re = Re2.create_exn "\\{(\\d+)\\.\\.(\\d+)\\}" in
  let rec expand name =
    match Re2.get_matches_exn ~max:1 re name with
    | [] -> [ name; ]
    | _::_::_ -> assert false
    | [m] ->
    match Re2.Match.get_all (Re2.without_trailing_none m) with
    | [| _; Some start; Some stop; |] ->
      let (offset, length) = Re2.Match.get_pos_exn ~sub:(`Index 0) m in
      let before = String.slice ~last:offset name in
      let after = String.slice ~first:(offset + length) name in
      let digits = String.length start in
      let start = int_of_string start in
      let stop = int_of_string stop in
      List.init (stop - start + 1) begin fun i ->
        expand (sprintf "%s%0*d%s" before digits (i + start) after)
      end |>
      List.concat
    | _ -> assert false
  in
  expand

let load_config () =
  let config_file = Filename.concat !!Nix.xdg_config_dir "es-cli/config.json" in
  match Sys.file_exists config_file with
  | false -> { Config_j.clusters = []; version = None; }
  | true ->
  let config = Control.with_input_txt config_file IO.read_all in
  Config_j.config_of_string config

let get_cluster config name =
  match List.assoc name config.Config_t.clusters with
  | { Config_t.host; nodes; version; } -> { host = Option.default name host; nodes; version; }
  | exception Not_found -> { host = name; nodes = None; version = None; }
