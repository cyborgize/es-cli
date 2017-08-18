open Prelude
open ExtLib
open Printf

module J = Yojson.Safe
module SS = Set.Make(String)

let log = Log.from "es"

let usage tools =
  fprintf stderr "Usage: %s {<tool>|-help|version}\n" Sys.executable_name;
  fprintf stderr "where <tool> is one of:\n";
  List.sort ~cmp:compare tools |>
  List.iter (fun (s,_) -> fprintf stderr "  %s\n" s)

let health () =
  let cmd = ref [] in
  let args = ExtArg.[
    "--", Rest (tuck cmd), " signal end of options";
  ] in
  ExtArg.parse ~f:(tuck cmd) args;
  let usage () = fprintf stderr "health [options] <host1 [host2 [host3...]]>\n"; exit 1 in
  match List.rev !cmd with
  | [] -> usage ()
  | hosts ->
  Lwt_main.run @@
  let%lwt results =
    Lwt_list.mapi_p begin fun i host ->
      let url = host ^ "/_cat/health?v" in
      match%lwt Web.http_request_lwt `GET url with
      | exception exn -> log #error ~exn "search"; Lwt.return (i, sprintf "%s: failure: %s" host (Printexc.to_string exn))
      | `Error error -> log #error "health error : %s" error; Lwt.return (i, sprintf "%s: error %s\n" host error)
      | `Ok result -> Lwt.return (i, result)
    end hosts
  in
  List.sort ~cmp:(Factor.Int.compare $$ fst) results |>
  Lwt_list.iter_s (fun (_i, result) -> Lwt_io.print result)

let nodes () =
  let cmd = ref [] in
  let check_nodes = ref [] in
  let args = ExtArg.[
    "-h", Rest (tuck check_nodes), "<node1 [node 2 [node 3...]]> #check presence of specified nodes";
    "--", Rest (tuck cmd), " signal end of options";
  ] in
  ExtArg.parse ~f:(tuck cmd) args;
  let usage () = fprintf stderr "nodes [options] <host>\n"; exit 1 in
  match List.rev !cmd with
  | [] | _::_::_ -> usage ()
  | [host] ->
  let check_nodes = SS.of_list !check_nodes in
  let url = host ^ "/_nodes" in
  Lwt_main.run @@
  match%lwt Web.http_request_lwt `GET url with
  | exception exn -> log #error ~exn "search"; Lwt.fail exn
  | `Error error -> log #error "nodes error : %s" error; Lwt.fail_with error
  | `Ok result ->
  J.from_string result |>
  J.Util.member "nodes" |>
  J.Util.to_assoc |>
  List.fold_left begin fun (missing, present) (_node_id, node) ->
    let name = J.Util.member "name" node |> J.Util.to_string in
    SS.remove name missing, SS.add name present
  end (check_nodes, SS.empty) |>
  fun (missing, present) ->
  let%lwt () =
    match SS.is_empty missing with
    | true -> Lwt.return_unit
    | false -> Lwt_io.printlf "missing: %s" (String.concat " " (SS.elements missing))
  in
  let%lwt () =
    let unlisted = SS.diff present check_nodes in
    match SS.is_empty unlisted with
    | true -> Lwt.return_unit
    | false -> Lwt_io.printlf "unlisted: %s" (String.concat " " (SS.elements unlisted))
  in
  Lwt.return_unit

let search () =
  let cmd = ref [] in
  let size = ref None in
  let from = ref None in
  let sort = ref [] in
  let source_include = ref [] in
  let source_exclude = ref [] in
  let show_count = ref false in
  let show_hits = ref false in
  let args = ExtArg.[
    may_int "n" size "<n> #set search limit";
    may_int "o" from "<n> #set search offset";
    "-s", String (tuck sort), "<field[:dir]> #set sort order";
    "-i", String (tuck source_include), "<field> #include source field";
    "-e", String (tuck source_exclude), "<field> #exclude source field";
    bool "c" show_count " output number of hits";
    bool "h" show_hits " output hit ids";
    "--", Rest (tuck cmd), " signal end of options";
  ] in
  ExtArg.parse ~f:(tuck cmd) args;
  let usage () = fprintf stderr "search [options] <host>/<index>/<doc_type> [query]\n"; exit 1 in
  match List.rev !cmd with
  | [] | _::_::_::_ -> usage ()
  | host :: query ->
  match Re2.Regex.split ~max:3 (Re2.Regex.create_exn "/") host with
  | [] -> assert false
  | [_host] -> usage ()
  | host :: index :: doc_type ->
  let one = function [] -> None | [x] -> Some x | _ -> assert false in
  let str = Option.map string_of_int in
  let csv = function [] -> None | l -> Some (String.concat "," l) in
  let args = [
    "size", str !size;
    "from", str !from;
    "sort", csv !sort;
    "_source", csv !source_include;
    "_source_exclude", csv !source_exclude;
    "q", one query;
  ] in
  let args = List.filter_map (function name, Some value -> Some (name, value) | _ -> None) args in
  let args = match args with [] -> "" | args -> "?" ^ Web.make_url_args args in
  let url = String.concat "/" (List.filter_map id [ Some host; Some index; one doc_type; Some ("_search" ^ args); ]) in
  Lwt_main.run @@
  match%lwt Web.http_request_lwt `POST url with
  | exception exn -> log #error ~exn "search"; Lwt.fail exn
  | `Error error -> log #error "search error : %s" error; Lwt.fail_with error
  | `Ok result ->
  match !show_count || !show_hits with
  | false -> Lwt_io.printl result
  | _ ->
  let%lwt () =
    match !show_count with
    | false -> Lwt.return_unit
    | true -> J.from_string result |> J.Util.member "hits" |> J.Util.member "total" |> J.Util.to_int |> string_of_int |> Lwt_io.printl
  in
  let%lwt () =
    match !show_hits with
    | false -> Lwt.return_unit
    | true ->
    J.from_string result |> J.Util.member "hits" |> J.Util.member "hits" |> J.Util.convert_each begin fun hit ->
      List.map ((^) "/" $ J.Util.to_string $ flip J.Util.member hit) [ "_index"; "_type"; "_id"; ] |> String.concat ""
    end |>
    Lwt_list.iter_s Lwt_io.printl
  in
  Lwt.return_unit

let () =
  let tools = [
    "health", health;
    "nodes", nodes;
    "search", search;
  ] in
  match Action.args with
  | [] | ("help" | "-help" | "--help") :: [] -> usage tools
  | "version"::[] -> print_endline Common.version
  | name :: _ ->
  match List.assoc name tools with
  | exception Not_found -> usage tools
  | tool ->
  incr Arg.current;
  tool ()
