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

let load_config_file () =
  let config_file = Filename.concat !!Nix.xdg_config_dir "es-cli/config.json" in
  match Sys.file_exists config_file with
  | true -> Some (Control.with_input_txt config_file IO.read_all)
  | false -> None

let load_config () =
  match load_config_file () with
  | Some config -> Config_j.config_of_string config
  | None -> { Config_j.clusters = []; version = None; }

let get_config_alias command =
  match load_config_file () with
  | exception _exn -> None
  | None -> None
  | Some config ->
  match Config_j.aliases_config_of_string config with
  | exception _exn -> None
  | { Config_t.aliases; } ->
  match List.assoc command aliases with
  | exception Not_found -> None
  | alias -> Some alias

let get_argv () =
  match Array.to_list Sys.argv with
  | [] | _ :: [] -> Sys.argv
  | exe :: command :: rest ->
  match get_config_alias command with
  | None -> Sys.argv
  | Some { Config_t.command; host; args; allow_no_host; } ->
  let rest =
    match host with
    | Some host -> host :: (args @ rest)
    | None ->
    match rest with
    | host :: rest -> host :: (args @ rest)
    | [] when allow_no_host -> args @ rest
    | [] -> []
  in
  Array.of_list (exe :: command :: rest)

let get_cluster config name =
  match List.assoc name config.Config_t.clusters with
  | { Config_t.host; nodes; version; aliases = _; } -> { host = Option.default name host; nodes; version; }
  | exception Not_found -> { host = name; nodes = None; version = None; }
