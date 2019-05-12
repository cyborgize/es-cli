open Devkit
open ExtLib
open Printf

module J = Yojson.Safe
module SS = Set.Make(String)

let log = Log.from "es"

let json_content_type = "application/json"

let cmd = ref []

let http_timeout = ref (Time.seconds 60)

type common_args = {
  es_version : Config_t.version option;
  verbose : bool;
}

let args =
  ExtArg.[
    "-T", String (fun t -> http_timeout := Time.of_compact_duration t), " set HTTP request timeout (format: 45s, 2m, or 1m30s)";
    "--", Rest (tuck cmd), " signal end of options";
  ]

type es_version_config = {
  source_includes_arg : string;
  source_excludes_arg : string;
  read_total : J.lexer_state -> Lexing.lexbuf -> Elastic_t.total;
  write_total : Bi_outbuf.t -> Elastic_t.total -> unit;
}

let es6_config = {
  source_includes_arg = "_source_include";
  source_excludes_arg = "_source_exclude";
  read_total = Elastic_j.read_es6_total;
  write_total = Elastic_j.write_es6_total;
}

let es7_config = {
  source_includes_arg = "_source_includes";
  source_excludes_arg = "_source_excludes";
  read_total = Elastic_j.read_total;
  write_total = Elastic_j.write_total;
}

let rec coalesce = function Some _ as hd :: _ -> hd | None :: tl -> coalesce tl | [] -> None

let get_es_version_config' = function
  | Some (`ES5 | `ES6) -> es6_config
  | None | Some `ES7 -> es7_config

let get_es_version_config es_version { Config_t.version = config_version; _ } cluster_version =
  get_es_version_config' (coalesce [ es_version; cluster_version; config_version; ])

let get_body_query_file body_query =
  match body_query <> "" && body_query.[0] = '@' with
  | true -> Control.with_input_txt (String.slice ~first:1 body_query) IO.read_all
  | false -> body_query

let usage tools =
  fprintf stderr "Usage: %s {<tool>|-help|version}\n" Sys.executable_name;
  fprintf stderr "where <tool> is one of:\n";
  List.sort ~cmp:compare tools |>
  List.iter (fun (s,_) -> fprintf stderr "  %s\n" s)

let str_list =
  ExtArg.make_arg @@ object
    method store v = Arg.String (tuck v)
    method kind = "string"
    method show v = match !v with [] -> "none" | l -> String.concat "," l
  end

let csv ?(sep=",") = function [] -> None | l -> Some (String.concat sep l)

let int = Option.map string_of_int

let one = function [] -> None | [x] -> Some x | _ -> assert false

let flag ?(default=false) = function x when x = default -> None | true -> Some "true" | false -> Some "false"

let map_of_hit_format =
  let open Elastic_t in function
  | "full_id" -> (fun ({ index; doc_type; id; _ } : 'a Elastic_t.option_hit) -> sprintf "/%s/%s/%s" index doc_type id)
  | "id" -> (fun hit -> hit.id)
  | "type" -> (fun hit -> hit.doc_type)
  | "index" -> (fun hit -> hit.index)
  | "routing" -> (fun hit -> Option.default "" hit.routing)
  | "hit" -> (fun hit -> Elastic_j.string_of_option_hit J.write_json hit)
  | "source" -> (fun { source; _ } -> Option.map_default J.to_string "" source)
  | s -> Exn.fail "unknown hit field \"%s\"" s

let map_of_index_shard_format =
  let open Elastic_t in function
  | "index" -> (fun index (_shard : index_shard) -> `String index)
  | "shard" -> (fun _index shard -> `Int shard.id)
  | "time" -> (fun _index shard -> `Duration (Time.msec shard.index.total_time_in_millis))
  | "type" -> (fun _index shard -> `Symbol shard.kind)
  | "stage" -> (fun _index shard -> `Symbol shard.stage)
  | "source_host" -> (fun _index shard -> match shard.source.host with Some host -> `String host | None -> `None)
  | "source_node" -> (fun _index shard -> match shard.source.name with Some name -> `String name | None -> `None)
  | "target_host" -> (fun _index shard -> match shard.target.host with Some host -> `String host | None -> `None)
  | "target_node" -> (fun _index shard -> match shard.target.name with Some name -> `String name | None -> `None)
  | "repository" -> (fun _index _shard -> `None) (* FIXME what is repository? *)
  | "snapshot" -> (fun _index _shard -> `None) (* FIXME what is snapshot? *)
  | "files" -> (fun _index shard -> `Int shard.index.files.total) (* FIXME what's the difference w/ files_total? *)
  | "files_recovered" -> (fun _index shard -> `Int shard.index.files.recovered)
  | "files_percent" -> (fun _index shard -> `String shard.index.files.percent)
  | "files_total" -> (fun _index shard -> `Int shard.index.files.total)
  | "bytes" -> (fun _index shard -> `Int shard.index.size.total_in_bytes) (* FIXME what's the difference w/ bytes_total? *)
  | "bytes_recovered" -> (fun _index shard -> `Int shard.index.size.recovered_in_bytes)
  | "bytes_percent" -> (fun _index shard -> `String shard.index.size.percent)
  | "bytes_total" -> (fun _index shard -> `Int shard.index.size.total_in_bytes)
  | "translog_ops" -> (fun _index shard -> `Int shard.translog.total)
  | "translog_ops_recovered" -> (fun _index shard -> `Int shard.translog.recovered)
  | "translog_ops_percent" -> (fun _index shard -> `String shard.translog.percent)
  | s -> Exn.fail "unknown index shard field \"%s\"" s

let default_index_shard_format = [
  "index"; "shard"; "time"; "type"; "stage";
  "source_host"; "source_node"; "target_host"; "target_node";
  "repository"; "snapshot";
  "files"; "files_recovered"; "files_percent"; "files_total";
  "bytes"; "bytes_recovered"; "bytes_percent"; "bytes_total";
  "translog_ops"; "translog_ops_recovered"; "translog_ops_percent";
]

let map_show = function
  | `String x | `Symbol x -> x
  | `Int x -> string_of_int x
  | `Float x -> string_of_float x
  | `Duration x -> Time.compact_duration x
  | `None -> "n/a"

let compare_fmt = function
  | `String x -> String.equal x
  | `Symbol x -> String.equal (String.lowercase_ascii x) $ String.lowercase_ascii
  | `Int x -> Factor.Int.equal x $ int_of_string
  | `Float x -> Factor.Float.equal x $ float_of_string
  | `Duration x -> Factor.Float.equal x $ float_of_string (* FIXME parse time? *)
  | `None -> (fun _ -> false)

let http_request_lwt' ?verbose ?body action url =
  Web.http_request_lwt' ?verbose ~timeout:(Time.to_sec !http_timeout) ?body action url

let http_request_lwt ?verbose ?body action url =
  Web.http_request_lwt ?verbose ~timeout:(Time.to_sec !http_timeout) ?body action url

type alias_action = {
  action : [ `Add | `Remove ];
  index : string;
  alias : string;
}

type alias_args = {
  host : string;
  actions : alias_action list;
}

let alias { verbose; _ } {
    host;
    actions;
  } =
  let config = Common.load_config () in
  let { Common.host; _ } = Common.get_cluster config host in
  let url = sprintf "%s/_aliases" host in
  let (action, body) =
    match actions with
    | [] -> `GET, None
    | actions ->
    let actions = List.map (fun { action; index; alias; } -> [ action, { Elastic_t.index; alias; }; ]) actions in
    `POST, Some (`Raw (json_content_type, Elastic_j.string_of_aliases { Elastic_t.actions; }))
  in
  Lwt_main.run @@
  match%lwt http_request_lwt ~verbose ?body action url with
  | exception exn -> log #error ~exn "alias"; Lwt.fail exn
  | `Error error -> log #error "alias error : %s" error; Lwt.fail_with error
  | `Ok result -> Lwt_io.printl result

let flush { verbose; _ } config =
  let force = ref false in
  let synced = ref false in
  let wait = ref false in
  let args =
    let open ExtArg in
    ("-f", Set force, " force flush") ::
    ("-s", Set synced, " synced flush") ::
    ("-w", Set wait, " wait if ongoing") ::
    args
  in
  ExtArg.parse ~f:(tuck cmd) args;
  let usage () = fprintf stderr "flush [options] <host> [index1 [index2 ...]]\n"; exit 1 in
  match List.rev !cmd with
  | [] -> usage ()
  | host :: indices ->
  let { Common.host; _ } = Common.get_cluster config host in
  let bool' v = function true -> Some v | false -> None in
  let bool = bool' "true" in
  let args = [
    "force", bool !force;
    "wait_if_ongoing", bool !wait;
  ] in
  let path = [
    Some host;
    csv indices;
    Some "_flush";
    bool' "synced" !synced;
  ] in
  let args = List.filter_map (function name, Some value -> Some (name, value) | _ -> None) args in
  let args = match args with [] -> "" | args -> "?" ^ Web.make_url_args args in
  let url = String.concat "/" (List.filter_map id path) ^ args in
  Lwt_main.run @@
  match%lwt http_request_lwt ~verbose `POST url with
  | exception exn -> log #error ~exn "flush"; Lwt.fail exn
  | `Error error -> log #error "flush error : %s" error; Lwt.fail_with error
  | `Ok result -> Lwt_io.printl result

let get { verbose; es_version; _ } config =
  let source_includes = ref [] in
  let source_excludes = ref [] in
  let routing = ref [] in
  let preference = ref [] in
  let format = ref [] in
  let args =
    str_list "i" source_includes "<field> #include source field" ::
    str_list "e" source_excludes "<field> #exclude source field" ::
    str_list "r" routing "<routing> #set routing" ::
    str_list "p" preference "<preference> #set preference" ::
    str_list "f" format "<hit|id|source> #map hit according to specified format" ::
    args
  in
  ExtArg.parse ~f:(tuck cmd) args;
  let usage () = fprintf stderr "get [options] <host>/<index>[/<doc_type>/<doc_id>]\n"; exit 1 in
  match List.rev !cmd with
  | [] | _::_::_ -> usage ()
  | [host] ->
  match Re2.split (Re2.create_exn "/") host with
  | [] -> assert false
  | _host :: ([] | [ _; _; ] | _::_::_::_::_) -> usage ()
  | host :: index :: doc ->
  let { Common.host; version; _ } = Common.get_cluster config host in
  let { source_includes_arg; source_excludes_arg; _ } = get_es_version_config es_version config version in
  let args = [
    (if !source_excludes = [] then "_source" else source_includes_arg), csv !source_includes;
    source_excludes_arg, csv !source_excludes;
    "routing", csv !routing;
    "preference", csv ~sep:"|" !preference;
  ] in
  let format =
    match doc with
    | [] -> []
    | _ ->
    List.rev_map (fun format -> List.map map_of_hit_format (Stre.nsplitc format ',')) !format |>
    List.concat
  in
  let args = List.filter_map (function name, Some value -> Some (name, value) | _ -> None) args in
  let args = match args with [] -> "" | args -> "?" ^ Web.make_url_args args in
  let url = String.concat "/" (host :: index :: doc) ^ args in
  Lwt_main.run @@
  match%lwt http_request_lwt' ~verbose `GET url with
  | exception exn -> log #error ~exn "get"; Lwt.fail exn
  | `Error code ->
    let error = sprintf "(%d) %s" (Curl.errno code) (Curl.strerror code) in
    log #error "get error : %s" error;
    Lwt.fail_with error
  | `Ok (code, result) ->
  let is_error_response result = Elastic_j.((response''_of_string result).error) <> None in
  let is_severe_error code result = code / 100 <> 2 && (code <> 404 || is_error_response result) in
  match is_severe_error code result with
  | exception exn -> log #error ~exn "get"; Lwt.fail exn
  | is_error when is_error || format = [] -> Lwt_io.printl result
  | _ ->
  match Elastic_j.option_hit_of_string J.read_json result with
  | exception exn -> log #error ~exn "get %s" result; Lwt.fail exn
  | { Elastic_t.found = false; _ } -> Lwt.return_unit
  | hit ->
  List.map (fun f -> f hit) format |>
  String.join " " |>
  Lwt_io.printl

let health { verbose; _ } config =
  ExtArg.parse ~f:(tuck cmd) args;
  let all_hosts = lazy (List.map (fun (name, _) -> Common.get_cluster config name) config.Config_t.clusters) in
  let hosts =
    match List.rev !cmd with
    | [] -> !!all_hosts
    | hosts ->
    List.map begin function
      | "_all" -> !!all_hosts
      | name -> [ Common.get_cluster config name; ]
    end hosts |>
    List.concat
  in
  Lwt_main.run @@
  let%lwt results =
    Lwt_list.mapi_p begin fun i { Common.host; _ } ->
      let columns = [
        "cluster"; "status";
        "node.total"; "node.data";
        "shards"; "pri"; "relo"; "init"; "unassign";
        "pending_tasks"; "max_task_wait_time";
        "active_shards_percent";
      ] in
      let url = sprintf "%s/_cat/health?h=%s" host (String.concat "," columns) in
      match%lwt http_request_lwt ~verbose `GET url with
      | exception exn -> log #error ~exn "health"; Lwt.return (i, sprintf "%s failure %s" host (Printexc.to_string exn))
      | `Error error -> log #error "health error : %s" error; Lwt.return (i, sprintf "%s error %s\n" host error)
      | `Ok result -> Lwt.return (i, sprintf "%s %s" host result)
    end hosts
  in
  List.sort ~cmp:(Factor.Int.compare $$ fst) results |>
  Lwt_list.iter_s (fun (_i, result) -> Lwt_io.print result)

let nodes { verbose; _ } config =
  let check_nodes = ref [] in
  let args =
    let open ExtArg in
    ("-h", Rest (tuck check_nodes), "<node1 [node 2 [node 3...]]> #check presence of specified nodes") ::
    args
  in
  ExtArg.parse ~f:(tuck cmd) args;
  let usage () = fprintf stderr "nodes [options] <host>\n"; exit 1 in
  match List.rev !cmd with
  | [] | _::_::_ -> usage ()
  | [host] ->
  let { Common.host; nodes; _ } = Common.get_cluster config host in
  let check_nodes = match !check_nodes with [] -> Option.default [] nodes | nodes -> nodes in
  let check_nodes = SS.of_list (List.concat (List.map Common.expand_node check_nodes)) in
  let url = host ^ "/_nodes" in
  Lwt_main.run @@
  match%lwt http_request_lwt ~verbose `GET url with
  | exception exn -> log #error ~exn "nodes"; Lwt.fail exn
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

let put { verbose; _ } config =
  let routing = ref None in
  let args =
    let open ExtArg in
    may_str "r" routing "<routing> #set routing" ::
    args
  in
  ExtArg.parse ~f:(tuck cmd) args;
  let usage () = fprintf stderr "put [options] <host>/<index>/<doc_type>[/<doc_id>] <document>\n"; exit 1 in
  match List.rev !cmd with
  | [] | _::_::_::_ -> usage ()
  | host :: body ->
  match Re2.split (Re2.create_exn "/") host with
  | [] -> assert false
  | _host :: ([] | [_] | _::_::_::_::_) -> usage ()
  | host :: index :: doc ->
  let { Common.host; _ } = Common.get_cluster config host in
  let args = [ "routing", !routing; ] in
  let args = List.filter_map (function name, Some value -> Some (name, value) | _ -> None) args in
  let args = match args with [] -> "" | args -> "?" ^ Web.make_url_args args in
  let url = String.concat "/" (host :: index :: doc) ^ args in
  Lwt_main.run @@
  let%lwt body = match body with [body] -> Lwt.return body | [] -> Lwt_io.read Lwt_io.stdin | _ -> assert%lwt false in
  match%lwt http_request_lwt ~verbose ~body:(`Raw (json_content_type, body)) `PUT url with
  | exception exn -> log #error ~exn "put"; Lwt.fail exn
  | `Error error -> log #error "put error : %s" error; Lwt.fail_with error
  | `Ok result -> Lwt_io.printl result

let recovery { verbose; _ } config =
  let two_str_list =
    ExtArg.make_arg @@ object
      method store v =
        let s1 = ref "" in
        Arg.(Tuple [ Set_string s1; String (fun s2 -> tuck v (!s1, s2)); ])
      method kind = "two strings"
      method show v =
        List.map (fun (s1, s2) -> s1 ^ " " ^ s2) !v |>
        String.concat " "
    end
  in
  let format = ref [] in
  let filter_include = ref [] in
  let filter_exclude = ref [] in
  let args =
    str_list "f" format "<index|shard|type|stage|...> #map hit according to specified format" ::
    two_str_list "i" filter_include "<column> <value> #include only shards matching the filter" ::
    two_str_list "e" filter_exclude "<column> <value> #exclude shards matching the filter" ::
    args
  in
  ExtArg.parse ~f:(tuck cmd) args;
  let usage () = fprintf stderr "recovery [options] <host> [<index1> [<index2> [<index3> ...]]]\n"; exit 1 in
  match List.rev !cmd with
  | [] -> usage ()
  | host :: indices ->
  let format = match !format with [] -> default_index_shard_format | format -> List.rev format in
  let format =
    List.map (fun format -> List.map map_of_index_shard_format (Stre.nsplitc format ',')) format |>
    List.concat
  in
  let filter_include = List.map (fun (k, v) -> map_of_index_shard_format k, v) !filter_include in
  let filter_exclude = List.map (fun (k, v) -> map_of_index_shard_format k, v) !filter_exclude in
  let { Common.host; _ } = Common.get_cluster config host in
  let url = String.concat "/" (List.filter_map id [ Some host; csv indices; Some "_recovery"; ]) in
  Lwt_main.run @@
  match%lwt http_request_lwt ~verbose `GET url with
  | exception exn -> log #error ~exn "recovery"; Lwt.fail exn
  | `Error error -> log #error "recovery error : %s" error; Lwt.fail_with error
  | `Ok result ->
  match Elastic_j.indices_shards_of_string result with
  | exception exn -> log #error ~exn "recovery %s" result; Lwt.fail exn
  | indices ->
  let indices =
    match filter_include, filter_exclude with
    | [], [] -> indices
    | _ ->
    List.map begin fun (index, { Elastic_t.shards; }) ->
      let shards =
        List.filter begin fun shard ->
          List.for_all (fun (f, v) -> compare_fmt (f index shard) v) filter_include &&
          not (List.exists (fun (f, v) -> compare_fmt (f index shard) v) filter_exclude)
        end shards
      in
      index, { Elastic_t.shards; }
    end indices
  in
  Lwt_list.iter_s begin fun (index, { Elastic_t.shards; }) ->
    Lwt_list.iter_s begin fun shard ->
      List.map (fun f -> map_show (f index shard)) format |>
      String.concat " " |>
      Lwt_io.printl
    end shards
  end indices

let refresh { verbose; _ } config =
  ExtArg.parse ~f:(tuck cmd) args;
  let usage () = fprintf stderr "refresh [options] <host> [<index1> [<index2> [<index3> ...]]]\n"; exit 1 in
  match List.rev !cmd with
  | [] -> usage ()
  | host :: indices ->
  let { Common.host; _ } = Common.get_cluster config host in
  let url = String.concat "/" (List.filter_map id [ Some host; csv indices; Some "_refresh"; ]) in
  Lwt_main.run @@
  match%lwt http_request_lwt ~verbose `POST url with
  | exception exn -> log #error ~exn "refresh"; Lwt.fail exn
  | `Error error -> log #error "refresh error : %s" error; Lwt.fail_with error
  | `Ok result -> Lwt_io.printl result

type search_args = {
  host : string;
  index : string;
  doc_type : string option;
  timeout : string option;
  size : int option;
  from : int option;
  sort : string list;
  source_includes : string list;
  source_excludes : string list;
  fields : string list;
  routing : string list;
  preference : string list;
  scroll : string option;
  slice_id : int option;
  slice_max : int option;
  query : string option;
  body_query : string option;
  analyzer : string option;
  analyze_wildcard : bool;
  default_field : string option;
  default_operator : string option;
  explain : bool;
  show_count : bool;
  track_total_hits : string option;
  retry : bool;
  format : string list;
}

let search { verbose; es_version; _ } {
    host;
    index;
    doc_type;
    timeout;
    size;
    from;
    sort;
    source_includes;
    source_excludes;
    fields;
    routing;
    preference;
    scroll;
    slice_id;
    slice_max;
    query;
    body_query;
    analyzer;
    analyze_wildcard;
    default_field;
    default_operator;
    explain;
    show_count;
    track_total_hits;
    retry;
    format;
  } =
  let config = Common.load_config () in
  let { Common.host; version; _ } = Common.get_cluster config host in
  let { source_includes_arg; source_excludes_arg; read_total; write_total; } = get_es_version_config es_version config version in
  let body_query = Option.map get_body_query_file body_query in
  let args = [
    "timeout", timeout;
    "size", int size;
    "from", int from;
    "track_total_hits", track_total_hits;
    "sort", csv sort;
    (if source_excludes = [] then "_source" else source_includes_arg), csv source_includes;
    source_excludes_arg, csv source_excludes;
    "stored_fields", csv fields;
    "routing", csv routing;
    "preference", csv ~sep:"|" preference;
    "explain", flag explain;
    "scroll", scroll;
    "analyzer", analyzer;
    "analyze_wildcard", flag analyze_wildcard;
    "df", default_field;
    "default_operator", default_operator;
    "q", query;
  ] in
  let format =
    List.rev_map (fun format -> List.map map_of_hit_format (Stre.nsplitc format ',')) format |>
    List.concat
  in
  let args = List.filter_map (function name, Some value -> Some (name, value) | _ -> None) args in
  let args = match args with [] -> "" | args -> "?" ^ Web.make_url_args args in
  let body_query =
    match slice_id, slice_max with
    | None, _ | _, None -> body_query
    | Some slice_id, Some slice_max ->
    let slice = "slice", `Assoc [ "id", `Int slice_id; "max", `Int slice_max; ] in
    match body_query with
    | None -> Some (Util_j.string_of_assoc [slice])
    | Some body ->
    let body = Util_j.assoc_of_string body in
    let body = slice :: List.filter (function "slice", _ -> false | _ -> true) body in
    Some (Util_j.string_of_assoc body)
  in
  let body = match body_query with Some query -> Some (`Raw (json_content_type, query)) | None -> None in
  let url = String.concat "/" (List.filter_map id [ Some host; Some index; doc_type; Some ("_search" ^ args); ]) in
  let htbl = Hashtbl.create (if retry then Option.default 10 size else 0) in
  let rec search () =
    match%lwt http_request_lwt ~verbose ?body `POST url with
    | exception exn -> log #error ~exn "search"; Lwt.fail exn
    | `Error error -> log #error "search error : %s" error; Lwt.fail_with error
    | `Ok result ->
    match show_count, format, scroll, retry with
    | false, [], None, false -> Lwt_io.printl result
    | show_count, format, scroll, retry ->
    let scroll_url = host ^ "/_search/scroll" in
    let clear_scroll' scroll_id =
      let clear_scroll = Elastic_j.string_of_clear_scroll { Elastic_t.scroll_id = [ scroll_id; ]; } in
      match%lwt http_request_lwt ~verbose ~body:(`Raw (json_content_type, clear_scroll)) `DELETE scroll_url with
      | `Error error -> log #error "clear scroll error : %s" error; Lwt.fail_with error
      | `Ok _ok -> Lwt.return_unit
    in
    let clear_scroll scroll_id = Option.map_default clear_scroll' Lwt.return_unit scroll_id in
    let rec loop result =
      let { Elastic_t.hits = response_hits; scroll_id; shards = { Elastic_t.failed; _ }; _ } as response =
        Elastic_j.response'_of_string (Elastic_j.read_option_hit J.read_json) read_total result
      in
      match response_hits with
      | None -> log #error "no hits"; clear_scroll scroll_id
      | Some ({ Elastic_t.total; hits; _ } as response_hits) ->
      let hits =
        match retry with
        | false -> hits
        | true ->
        List.filter begin fun ({ Elastic_t.index; doc_type; id; _ } : 'a Elastic_t.option_hit) ->
          let key = index, doc_type, id in
          match Hashtbl.mem htbl key with
          | false -> Hashtbl.add htbl key (); true
          | true -> false
        end hits
      in
      let%lwt () =
        match show_count with
        | false -> Lwt.return_unit
        | true ->
        match total with
        | None -> Lwt_io.printl "unknown"
        | Some { value; relation; } ->
        match relation with
        | `Eq -> Lwt_io.printlf "%d" value
        | `Gte -> Lwt_io.printlf ">=%d" value
      in
      let%lwt () =
        match format, show_count, retry with
        | [], true, _ -> Lwt.return_unit
        | [], false, false -> Lwt_io.printl result
        | [], false, true when hits <> [] || Hashtbl.length htbl = 0 ->
          { response with Elastic_t.hits = Some { response_hits with Elastic_t.hits; }; } |>
          Elastic_j.string_of_response' (Elastic_j.write_option_hit J.write_json) write_total |>
          Lwt_io.printl
        | _ ->
        Lwt_list.iter_s begin fun hit ->
          List.map (fun f -> f hit) format |>
          String.join " " |>
          Lwt_io.printl
        end hits
      in
      match failed > 0 && retry with
      | true ->
        let%lwt () = clear_scroll scroll_id in
        search ()
      | false ->
      match hits, scroll, scroll_id with
      | [], _, _ | _, None, _ | _, _, None -> clear_scroll scroll_id
      | _, Some scroll, Some scroll_id ->
      let scroll = Elastic_j.string_of_scroll { Elastic_t.scroll; scroll_id; } in
      match%lwt http_request_lwt ~verbose ~body:(`Raw (json_content_type, scroll)) `POST scroll_url with
      | `Error error ->
        log #error "scroll error : %s" error;
        let%lwt () = clear_scroll' scroll_id in
        Lwt.fail_with error
      | `Ok result -> loop result
    in
    loop result
  in
  Lwt_main.run @@ search ()

open Cmdliner

let common_args =
  let args es_version verbose = { es_version; verbose; } in
  let docs = Manpage.s_common_options in
  let es_version =
    Arg.(last & vflag_all [ None; ] [
      Some `ES5, Arg.info [ "5"; ] ~docs ~doc:"force ES version 5.x";
      Some `ES6, Arg.info [ "6"; ] ~docs ~doc:"force ES version 6.x";
      Some `ES7, Arg.info [ "7"; ] ~docs ~doc:"force ES version 7.x";
    ])
  in
  let verbose =
    let doc = "verbose output" in
    Arg.(value & flag & info [ "v"; "verbose"; ] ~docs ~doc)
  in
  Term.(const args $ es_version $ verbose)

let default_tool =
  let doc = "a command-line client for ES" in
  let sdocs = Manpage.s_common_options in
  let exits = Term.default_exits in
  let man = [] in
  Term.(ret (const (fun _ -> `Help (`Pager, None)) $ common_args), info "es" ~version:Common.version ~doc ~sdocs ~exits ~man)

let alias_tool =
  let alias
      common_args
      host
      index
      add
      remove
    =
    let map action = function
      | alias, Some index -> { action; index; alias; }
      | alias, None ->
      match index with
      | Some index -> { action; index; alias; }
      | None -> Exn.fail "INDEX is not specified for %s" alias
    in
    let add = List.map (map `Add) add in
    let remove = List.map (map `Remove) remove in
    alias common_args {
      host;
      actions = add @ remove;
    }
  in
  let action =
    let parse x =
      match Stre.splitc x '=' with
      | alias, index -> Ok (alias, Some index)
      | exception Not_found -> Ok (x, None)
    in
    let print fmt (alias, index) =
      match index with
      | Some index -> Format.fprintf fmt "%s=%s" alias index
      | None -> Format.fprintf fmt "%s" alias
    in
    Arg.conv (parse, print)
  in
  let host = Arg.(required & pos 0 (some string) None & info [] ~docv:"HOST" ~doc:"host") in
  let index =
    let doc = "index to operate on. If not provided, -a and -r must include the =INDEX part." in
    Arg.(value & pos 1 (some string) None & info [] ~docv:"INDEX" ~doc)
  in
  let add =
    let doc = "add index INDEX to alias ALIAS" in
    Arg.(value & opt_all action [] & info [ "a"; "add"; ] ~docv:"ALIAS[=INDEX]" ~doc)
  in
  let remove =
    let doc = "remove index INDEX from alias ALIAS" in
    Arg.(value & opt_all action [] & info [ "r"; "remove"; ] ~docv:"ALIAS[=INDEX]" ~doc)
  in
  let open Term in
  const alias $
    common_args $
    host $
    index $
    add $
    remove,
  let doc = "add or remove index aliases" in
  let exits = default_exits in
  let man = [] in
  info "alias" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let search_tool =
  let search
      common_args
      host
      index
      doc_type
      timeout
      size
      from
      sort
      source_includes
      source_excludes
      fields
      routing
      preference
      scroll
      slice_id
      slice_max
      query
      body_query
      analyzer
      analyze_wildcard
      default_field
      default_operator
      explain
      show_count
      track_total_hits
      retry
      format
    =
    search common_args {
      host;
      index;
      doc_type;
      timeout;
      size;
      from;
      sort;
      source_includes;
      source_excludes;
      fields;
      routing;
      preference;
      scroll;
      slice_id;
      slice_max;
      query;
      body_query;
      analyzer;
      analyze_wildcard;
      default_field;
      default_operator;
      explain;
      show_count;
      track_total_hits;
      retry;
      format;
    }
  in
  let host = Arg.(required & pos 0 (some string) None & info [] ~docv:"HOST" ~doc:"host") in
  let index = Arg.(required & pos 1 (some string) None & info [] ~docv:"INDEX" ~doc:"index") in
  let doc_type = Arg.(value & opt (some string) None & info [ "T"; "doctype"; ] ~docv:"DOC_TYPE" ~doc:"doctype") in
  let timeout = Arg.(value & opt (some string) None & info [ "t"; "timeout"; ] ~doc:"timeout") in
  let size = Arg.(value & opt (some int) None & info [ "n"; "size"; ] ~doc:"size") in
  let from = Arg.(value & opt (some int) None & info [ "o"; "from"; ] ~doc:"from") in
  let sort = Arg.(value & opt_all string [] & info [ "s"; "sort"; ] ~doc:"sort") in
  let source_includes = Arg.(value & opt_all string [] & info [ "i"; "source-includes"; ] ~doc:"source_includes") in
  let source_excludes = Arg.(value & opt_all string [] & info [ "e"; "source-excludes"; ] ~doc:"source_excludes") in
  let fields = Arg.(value & opt_all string [] & info [ "F"; "fields"; ] ~doc:"fields") in
  let routing = Arg.(value & opt_all string [] & info [ "r"; "routing"; ] ~doc:"routing") in
  let preference = Arg.(value & opt_all string [] & info [ "p"; "preference"; ] ~doc:"preference") in
  let scroll = Arg.(value & opt (some string) None & info [ "S"; "scroll"; ] ~doc:"scroll") in
  let slice_max = Arg.(value & opt (some int) None & info [ "N"; "slice-max"; ] ~doc:"slice_max") in
  let slice_id = Arg.(value & opt (some int) None & info [ "I"; "slice-id"; ] ~doc:"slice_id") in
  let query = Arg.(value & opt (some string) None & info [ "q"; "query"; ] ~doc:"query using query_string query") in
  let body_query = Arg.(value & pos 2 (some string) None & info [] ~docv:"BODY_QUERY" ~doc:"body query") in
  let analyzer = Arg.(value & opt (some string) None & info [ "a"; "analyzer"; ] ~doc:"analyzer to be used for query_string query") in
  let analyze_wildcard = Arg.(value & flag & info [ "w"; "analyze-wildcard"; ] ~doc:"analyze wildcard and prefix queries in query_string query") in
  let default_field = Arg.(value & opt (some string) None & info [ "d"; "default-field"; ] ~doc:"default field to be used for query_string query") in
  let default_operator = Arg.(value & opt (some string) None & info [ "O"; "default-operator"; ] ~doc:"default operator to be used for query_string query") in
  let explain = Arg.(value & flag & info [ "E"; "explain"; ] ~doc:"explain hits") in
  let show_count = Arg.(value & flag & info [ "c"; "show-count"; ] ~doc:"output total number of hits") in
  let track_total_hits = Arg.(value & opt (some string) None & info [ "C"; "track-total-hits"; ] ~doc:"track total number hits (true, false, or a number)") in
  let retry = Arg.(value & flag & info [ "R"; "retry"; ] ~doc:"retry if there are any failed shards") in
  let format = Arg.(value & opt_all string [] & info [ "f"; "format"; ] ~doc:"map hits according to specified format (hit|id|source)") in
  let open Term in
  const search $
    common_args $
    host $
    index $
    doc_type $
    timeout $
    size $
    from $
    sort $
    source_includes $
    source_excludes $
    fields $
    routing $
    preference $
    scroll $
    slice_id $
    slice_max $
    query $
    body_query $
    analyzer $
    analyze_wildcard $
    default_field $
    default_operator $
    explain $
    show_count $
    track_total_hits $
    retry $
    format,
  let doc = "search" in
  let exits = default_exits in
  let man = [] in
  info "search" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let tools = [
  alias_tool;
(*
  flush_tool;
  get_tool;
  health_tool;
  nodes_tool;
  put_tool;
  recovery_tool;
  refresh_tool;
*)
  search_tool;
]

let () = Term.(exit (eval_choice default_tool tools))
