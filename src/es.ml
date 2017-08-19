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

let health config =
  let cmd = ref [] in
  let args = ExtArg.[
    "--", Rest (tuck cmd), " signal end of options";
  ] in
  ExtArg.parse ~f:(tuck cmd) args;
  let all_hosts = lazy (List.map (fun (name, _) -> Common.get_host config name) config.Config_j.clusters) in
  let hosts =
    match List.rev !cmd with
    | [] -> !!all_hosts
    | hosts ->
    List.map begin function
      | "_all" -> !!all_hosts
      | name -> [ Common.get_host config name; ]
    end hosts |>
    List.concat
  in
  Lwt_main.run @@
  let%lwt results =
    Lwt_list.mapi_p begin fun i host ->
      let columns = [
        "cluster"; "status";
        "node.total"; "node.data";
        "shards"; "pri"; "relo"; "init"; "unassign";
        "pending_tasks"; "max_task_wait_time";
        "active_shards_percent";
      ] in
      let url = sprintf "%s/_cat/health?h=%s" host (String.concat "," columns) in
      match%lwt Web.http_request_lwt `GET url with
      | exception exn -> log #error ~exn "search"; Lwt.return (i, sprintf "%s failure %s" host (Printexc.to_string exn))
      | `Error error -> log #error "health error : %s" error; Lwt.return (i, sprintf "%s error %s\n" host error)
      | `Ok result -> Lwt.return (i, sprintf "%s %s" host result)
    end hosts
  in
  List.sort ~cmp:(Factor.Int.compare $$ fst) results |>
  Lwt_list.iter_s (fun (_i, result) -> Lwt_io.print result)

let nodes config =
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
  let (host, cluster) = Common.get_cluster config host in
  let check_nodes =
    match !check_nodes, cluster with
    | [], Some { Config_j.nodes = Some nodes; _ } -> nodes
    | nodes, _ -> nodes
  in
  let check_nodes = SS.of_list (List.concat (List.map Common.expand_node check_nodes)) in
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

let search config =
  let cmd = ref [] in
  let size = ref None in
  let from = ref None in
  let sort = ref [] in
  let source_include = ref [] in
  let source_exclude = ref [] in
  let routing = ref [] in
  let preference = ref [] in
  let scroll = ref None in
  let show_count = ref false in
  let show_hits = ref false in
  let str_list =
    ExtArg.make_arg @@ object
      method store v = Arg.String (tuck v)
      method kind = "string"
      method show v = match !v with [] -> "none" | l -> String.concat "," l
    end
  in
  let args = ExtArg.[
    may_int "n" size "<n> #set search limit";
    may_int "o" from "<n> #set search offset";
    str_list "s" sort "<field[:dir]> #set sort order";
    str_list "i" source_include "<field> #include source field";
    str_list "e" source_exclude "<field> #exclude source field";
    str_list "r" routing "<routing> #set routing";
    str_list "p" preference "<preference> #set preference";
    may_str "scroll" scroll "<interval> #scroll search";
    bool "c" show_count " output number of hits";
    bool "h" show_hits " output hit ids";
    "--", Rest (tuck cmd), " signal end of options";
  ] in
  ExtArg.parse ~f:(tuck cmd) args;
  let usage () = fprintf stderr "search [options] <host>/<index>[/<doc_type>] [query]\n"; exit 1 in
  match List.rev !cmd with
  | [] | _::_::_::_ -> usage ()
  | host :: query ->
  match Re2.Regex.split ~max:3 (Re2.Regex.create_exn "/") host with
  | [] -> assert false
  | [_host] -> usage ()
  | host :: index :: doc_type ->
  let host = Common.get_host config host in
  let one = function [] -> None | [x] -> Some x | _ -> assert false in
  let int = Option.map string_of_int in
  let csv ?(sep=",") = function [] -> None | l -> Some (String.concat sep l) in
  let args = [
    "size", int !size;
    "from", int !from;
    "sort", csv !sort;
    "_source", csv !source_include;
    "_source_exclude", csv !source_exclude;
    "routing", csv !routing;
    "preference", csv ~sep:"|" !routing;
    "scroll", !scroll;
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
  match !show_count || !show_hits, !scroll with
  | false, None -> Lwt_io.printl result
  | _ ->
  let json = "application/json" in
  let scroll_url = host ^ "/_search/scroll" in
  let clear_scroll = function
    | None -> Lwt.return_unit
    | Some scroll_id ->
    let clear_scroll = Elastic_j.string_of_clear_scroll { Elastic_j.scroll_id = [ scroll_id; ]; } in
    match%lwt Web.http_request_lwt ~body:(`Raw (json, clear_scroll)) `DELETE scroll_url with
    | `Error error -> log #error "clear scroll error : %s" error; Lwt.fail_with error
    | `Ok _ok -> Lwt.return_unit
  in
  let rec loop result =
    let { Elastic_j.hits; scroll_id; _ } = Elastic_j.response'_of_string Elastic_j.read_id_hit result in
    match hits with
    | None -> log #error "no hits"; clear_scroll scroll_id
    | Some { Elastic_j.total; hits; _ } ->
    let%lwt () =
      match !show_count with
      | false -> Lwt.return_unit
      | true -> Lwt_io.printlf "%d" total
    in
    let%lwt () =
      match !show_hits with
      | false -> Lwt.return_unit
      | true ->
      List.map (fun { Elastic_j.index; doc_type; id; } -> sprintf "/%s/%s/%s" index doc_type id) hits |>
      Lwt_list.iter_s Lwt_io.printl
    in
    let%lwt () =
      match !show_count || !show_hits with
      | true -> Lwt.return_unit
      | false -> Lwt_io.printl result
    in
    match hits, !scroll, scroll_id with
    | [], _, _ | _, None, _ | _, _, None -> clear_scroll scroll_id
    | _, Some scroll, Some scroll_id ->
    let scroll = Elastic_j.string_of_scroll { Elastic_j.scroll; scroll_id; } in
    match%lwt Web.http_request_lwt ~body:(`Raw (json, scroll)) `POST scroll_url with
    | `Error error ->
      log #error "scroll error : %s" error;
      let%lwt () = clear_scroll (Some scroll_id) in
      Lwt.fail_with error
    | `Ok result -> loop result
  in
  loop result

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
  let config = Common.load_config () in
  incr Arg.current;
  tool config
