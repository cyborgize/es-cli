open Prelude
open ExtLib
open Printf

module J = Yojson.Safe

let log = Log.from "common"

let version = sprintf "%s [%s@%s]" Version.id Version.user Version.host

let load_config () =
  let config_file = Filename.concat !!Nix.xdg_config_dir "es-cli/config.json" in
  match Sys.file_exists config_file with
  | false -> { Config_j.clusters = []; }
  | true ->
  let config = Control.with_input_txt config_file IO.read_all in
  Config_j.config_of_string config

let get_host config name =
  match List.assoc name config.Config_j.clusters with
  | exception Not_found -> name
  | { Config_j.host; _ } -> Option.default name host

let get_cluster config name =
  match List.assoc name config.Config_j.clusters with
  | exception Not_found -> name, None
  | { Config_j.host; _ } as cluster_config -> Option.default name host, Some cluster_config
