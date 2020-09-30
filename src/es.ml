open Devkit
open ExtLib
open Printf

module J = Yojson.Safe
module SS = Set.Make(String)

let log = Log.from "es"

let http_timeout = ref (Time.seconds 60)

type common_args = {
  es_version : Config_t.version option;
  verbose : bool;
}

let args =
  ExtArg.[
    "-T", String (fun t -> http_timeout := Time.of_compact_duration t), " set HTTP request timeout (format: 45s, 2m, or 1m30s)";
  ]

let json_content_type = "application/json"

let ndjson_content_type = "application/x-ndjson"

type content_type =
  | JSON of string
  | NDJSON of string

let json_body_opt = function
  | Some JSON body -> Some (`Raw (json_content_type, body))
  | Some NDJSON body -> Some (`Raw (ndjson_content_type, body))
  | None -> None

let make_url host path args =
  let args = List.filter_map (function name, Some value -> Some (name, value) | _ -> None) args in
  let path = String.concat "/" ("" :: List.filter_map id path) in
  let path = String.concat "?" (path :: match args with [] -> [] | _ -> [ Web.make_url_args args; ]) in
  String.concat "" [ host; path; ]

let request ?verbose ?body action host path args =
  Web.http_request_lwt' ?verbose ~timeout:(Time.to_sec !http_timeout) ?body:(json_body_opt body) action (make_url host path args)

let request ?verbose ?body action host path args unformat =
  match%lwt request ?verbose ?body action host path args with
  | `Error code -> Exn_lwt.fail "(%d) %s" (Curl.errno code) (Curl.strerror code)
  | `Ok (code, result) ->
  let is_error_response result = Elastic_j.((response''_of_string result).error) <> None in
  let is_severe_error code result = code / 100 <> 2 && (code <> 404 || is_error_response result) in
  match is_severe_error code result with
  | exception exn -> Exn_lwt.fail ~exn "http %d : %s" code result
  | true -> Lwt.return_error result
  | false ->
  match unformat result with
  | exception exn -> Exn_lwt.fail ~exn "unformat %s" result
  | docs -> Lwt.return_ok docs

exception ErrorExit

let fail_lwt fmt =
  ksprintf begin fun s ->
    let%lwt () = Lwt_io.eprintl s in
    Lwt.fail ErrorExit
  end fmt

type 't json_reader = J.lexer_state -> Lexing.lexbuf -> 't

type 't json_writer = Bi_outbuf.t -> 't -> unit

type es_version_config = {
  read_total : Elastic_t.total json_reader;
  write_total : Elastic_t.total json_writer;
  default_get_doc_type : string;
  default_put_doc_type : string option;
}

let es6_config = {
  read_total = Elastic_j.read_es6_total;
  write_total = Elastic_j.write_es6_total;
  default_get_doc_type = "_all";
  default_put_doc_type = None;
}

let es7_config = {
  read_total = Elastic_j.read_total;
  write_total = Elastic_j.write_total;
  default_get_doc_type = "_doc";
  default_put_doc_type = Some "_doc";
}

let rec coalesce = function Some _ as hd :: _ -> hd | None :: tl -> coalesce tl | [] -> None

let get_es_version { verbose; _ } host =
  match%lwt request ~verbose `GET host [] [] Elastic_j.main_of_string with
  | Error error -> fail_lwt "could not get ES version:\n%s" error
  | Ok { Elastic_t.version = { number; }; } ->
  match Stre.nsplitc number '.' with
  | [] -> Exn_lwt.fail "empty ES version number"
  | "5" :: _ -> Lwt.return `ES5
  | "6" :: _ -> Lwt.return `ES6
  | "7" :: _ -> Lwt.return `ES7
  | other :: _ ->
  match int_of_string other with
  | exception exn -> Exn_lwt.fail ~exn "invalid ES version number : %s" number
  | _ -> Exn_lwt.fail "unsupported ES version number : %s" number

let get_es_version_config' = function
  | `ES5 | `ES6 -> es6_config
  | `ES7 -> es7_config

let get_es_version_config common_args host es_version { Config_t.version = config_version; _ } cluster_version =
  let version = coalesce [ es_version; cluster_version; config_version; ] in
  let%lwt version =
    match version with
    | Some (#Wrap.Version.exact as version) -> Lwt.return version
    | None | Some `Auto -> get_es_version common_args host
  in
  Lwt.return (get_es_version_config' version)

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

type hit_format = [
  | `FullID
  | `ID
  | `Type
  | `Index
  | `Routing
  | `Hit
  | `Source
]

type hit_formatter = J.t Elastic_t.option_hit -> string

let hit_format_of_string = function
  | "full_id" -> `FullID
  | "id" -> `ID
  | "type" -> `Type
  | "index" -> `Index
  | "routing" -> `Routing
  | "hit" -> `Hit
  | "source" -> `Source
  | s -> Exn.fail "unknown hit field \"%s\"" s

let string_of_hit_format = function
  | `FullID -> "full_id"
  | `ID -> "id"
  | `Type -> "type"
  | `Index -> "index"
  | `Routing -> "routing"
  | `Hit -> "hit"
  | `Source -> "source"

let map_of_hit_format =
  let open Elastic_t in function
  | `FullID -> (fun ({ index; doc_type; id; _ } : 'a Elastic_t.option_hit) -> sprintf "/%s/%s/%s" index doc_type id)
  | `ID -> (fun hit -> hit.id)
  | `Type -> (fun hit -> hit.doc_type)
  | `Index -> (fun hit -> hit.index)
  | `Routing -> (fun hit -> Option.default "" hit.routing)
  | `Hit -> (fun hit -> Elastic_j.string_of_option_hit J.write_json hit)
  | `Source -> (fun { source; _ } -> Option.map_default J.to_string "" source)

type index_shard_format = [
  | `Index
  | `Shard
  | `Time
  | `Type
  | `Stage
  | `SourceHost
  | `SourceNode
  | `TargetHost
  | `TargetNode
  | `Repository
  | `Snapshot
  | `Files
  | `FilesRecovered
  | `FilesPercent
  | `FilesTotal
  | `Bytes
  | `BytesRecovered
  | `BytesPercent
  | `BytesTotal
  | `TranslogOps
  | `TranslogOpsRecovered
  | `TranslogOpsPercent
]

let index_shard_format_of_string = function
  | "index" -> `Index
  | "shard" -> `Shard
  | "time" -> `Time
  | "type" -> `Type
  | "stage" -> `Stage
  | "source_host" -> `SourceHost
  | "source_node" -> `SourceNode
  | "target_host" -> `TargetHost
  | "target_node" -> `TargetNode
  | "repository" -> `Repository
  | "snapshot" -> `Snapshot
  | "files" -> `Files
  | "files_recovered" -> `FilesRecovered
  | "files_percent" -> `FilesPercent
  | "files_total" -> `FilesTotal
  | "bytes" -> `Bytes
  | "bytes_recovered" -> `BytesRecovered
  | "bytes_percent" -> `BytesPercent
  | "bytes_total" -> `BytesTotal
  | "translog_ops" -> `TranslogOps
  | "translog_ops_recovered" -> `TranslogOpsRecovered
  | "translog_ops_percent" -> `TranslogOpsPercent
  | s -> Exn.fail "unknown index shard field \"%s\"" s

let string_of_index_shard_format = function
  | `Index -> "index"
  | `Shard -> "shard"
  | `Time -> "time"
  | `Type -> "type"
  | `Stage -> "stage"
  | `SourceHost -> "source_host"
  | `SourceNode -> "source_node"
  | `TargetHost -> "target_host"
  | `TargetNode -> "target_node"
  | `Repository -> "repository"
  | `Snapshot -> "snapshot"
  | `Files -> "files"
  | `FilesRecovered -> "files_recovered"
  | `FilesPercent -> "files_percent"
  | `FilesTotal -> "files_total"
  | `Bytes -> "bytes"
  | `BytesRecovered -> "bytes_recovered"
  | `BytesPercent -> "bytes_percent"
  | `BytesTotal -> "bytes_total"
  | `TranslogOps -> "translog_ops"
  | `TranslogOpsRecovered -> "translog_ops_recovered"
  | `TranslogOpsPercent -> "translog_ops_percent"

let map_of_index_shard_format =
  let open Elastic_t in function
  | `Index -> (fun index (_shard : index_shard) -> `String index)
  | `Shard -> (fun _index shard -> `Int shard.id)
  | `Time -> (fun _index shard -> `Duration (Time.msec shard.index.total_time_in_millis))
  | `Type -> (fun _index shard -> `Symbol shard.kind)
  | `Stage -> (fun _index shard -> `Symbol shard.stage)
  | `SourceHost -> (fun _index shard -> match shard.source.host with Some host -> `String host | None -> `None)
  | `SourceNode -> (fun _index shard -> match shard.source.name with Some name -> `String name | None -> `None)
  | `TargetHost -> (fun _index shard -> match shard.target.host with Some host -> `String host | None -> `None)
  | `TargetNode -> (fun _index shard -> match shard.target.name with Some name -> `String name | None -> `None)
  | `Repository -> (fun _index _shard -> `None) (* FIXME what is repository? *)
  | `Snapshot -> (fun _index _shard -> `None) (* FIXME what is snapshot? *)
  | `Files -> (fun _index shard -> `Int shard.index.files.total) (* FIXME what's the difference w/ files_total? *)
  | `FilesRecovered -> (fun _index shard -> `Int shard.index.files.recovered)
  | `FilesPercent -> (fun _index shard -> `String shard.index.files.percent)
  | `FilesTotal -> (fun _index shard -> `Int shard.index.files.total)
  | `Bytes -> (fun _index shard -> `Int shard.index.size.total_in_bytes) (* FIXME what's the difference w/ bytes_total? *)
  | `BytesRecovered -> (fun _index shard -> `Int shard.index.size.recovered_in_bytes)
  | `BytesPercent -> (fun _index shard -> `String shard.index.size.percent)
  | `BytesTotal -> (fun _index shard -> `Int shard.index.size.total_in_bytes)
  | `TranslogOps -> (fun _index shard -> `Int shard.translog.total)
  | `TranslogOpsRecovered -> (fun _index shard -> `Int shard.translog.recovered)
  | `TranslogOpsPercent -> (fun _index shard -> `String shard.translog.percent)

let default_index_shard_format = [
  `Index; `Shard; `Time; `Type; `Stage;
  `SourceHost; `SourceNode; `TargetHost; `TargetNode;
  `Repository; `Snapshot;
  `Files; `FilesRecovered; `FilesPercent; `FilesTotal;
  `Bytes; `BytesRecovered; `BytesPercent; `BytesTotal;
  `TranslogOps; `TranslogOpsRecovered; `TranslogOpsPercent;
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

let split doc_id = Stre.nsplitc doc_id '/'

let join doc_id = String.concat "/" doc_id

let is_pure_id' doc_id =
  match doc_id with
  | [ _doc_id; ] | [ ""; _doc_id; ] -> true
  | _ -> false

let is_pure_id doc_id = is_pure_id' (split doc_id)

let map_index_doc_id' doc_type doc_id =
  match doc_id with
  | [ doc_id; ] | [ ""; doc_id; ] -> Exn_lwt.fail "document id missing index name : /%s" doc_id
  | [ index; doc_id; ] | [ ""; index; doc_id; ] -> Lwt.return (index, doc_type, doc_id)
  | [ index; doc_type; doc_id; ] | [ ""; index; doc_type; doc_id; ] -> Lwt.return (index, Some doc_type, doc_id)
  | _ -> Exn_lwt.fail "invalid document id : %s" (join doc_id)

let map_index_doc_id doc_type doc_id = map_index_doc_id' doc_type (split doc_id)

let map_doc_id' index doc_type doc_id =
  match doc_id with
  | [ doc_id; ] | [ ""; doc_id; ] -> Lwt.return (index, doc_type, doc_id)
  | [ doc_type; doc_id; ] | [ ""; doc_type; doc_id; ] -> Lwt.return (index, Some doc_type, doc_id)
  | _ -> Exn_lwt.fail "invalid document id : /%s/%s" index (join doc_id)

let map_doc_id index doc_type doc_id = map_doc_id' index doc_type (split doc_id)

let map_doc_id_opt' index doc_type doc_id =
  let%lwt (index, doc_type, doc_id) = map_doc_id' index doc_type doc_id in
  Lwt.return (index, doc_type, Some doc_id)

let map_doc_id_opt index doc_type doc_id = map_doc_id_opt' index doc_type (split doc_id)

let map_typed_doc_id' index doc_type doc_id =
  match doc_id with
  | [ doc_id; ] | [ ""; doc_id; ] -> Lwt.return (index, Some doc_type, doc_id)
  | _ -> Exn_lwt.fail "invalid document id : /%s/%s/%s" index doc_type (join doc_id)

let map_typed_doc_id index doc_type doc_id = map_typed_doc_id' index doc_type (split doc_id)

let map_typed_doc_id_opt' index doc_type doc_id =
  let%lwt (index, doc_type, doc_id) = map_typed_doc_id' index doc_type doc_id in
  Lwt.return (index, doc_type, Some doc_id)

let map_typed_doc_id_opt index doc_type doc_id = map_typed_doc_id_opt' index doc_type (split doc_id)

let map_index_mode index =
  match Stre.nsplitc index '/' with
  | [ index; ] | [ ""; index; ] -> `Index index
  | [ index; doc_type_or_id; ] | [ ""; index; doc_type_or_id; ] -> `IndexOrID (index, doc_type_or_id)
  | [ index; doc_type; doc_id; ] | [ ""; index; doc_type; doc_id; ] -> `ID (index, doc_type, doc_id)
  | _ -> Exn.fail "invalid index name or document id : %s" index

let map_ids ~default_get_doc_type index doc_type doc_ids =
  let multiple (first_index, first_doc_type, _doc_id as first_doc) other_doc_ids =
    let first_doc_type = Option.default default_get_doc_type first_doc_type in
    let merge_equal x y = match x with Some x' when String.equal x' y -> x | _ -> None in
    let (docs, common_index, common_doc_type) =
      List.fold_left begin fun (docs, common_index, common_doc_type) (index, doc_type, _doc_id as doc) ->
        let common_index = merge_equal common_index index in
        let common_doc_type = merge_equal common_doc_type (Option.default default_get_doc_type doc_type) in
        doc :: docs, common_index, common_doc_type
      end ([ first_doc; ], Some first_index, Some first_doc_type) other_doc_ids
    in
    let (docs, index, doc_type) =
      match common_index, common_doc_type with
      | Some _, Some _ ->
        let docs = List.map (fun (_index, _doc_type, doc_id) -> None, None, doc_id) docs in
        docs, common_index, common_doc_type
      | Some _, None ->
        let docs = List.map (fun (_index, doc_type, doc_id) -> None, doc_type, doc_id) docs in
        docs, common_index, None
      | None, _ ->
        let docs = List.map (fun (index, doc_type, doc_id) -> Some index, doc_type, doc_id) docs in
        docs, None, None
    in
    Lwt.return (`Multi (docs, index, doc_type))
  in
  let%lwt mode = Lwt.wrap1 map_index_mode index in
  let doc_ids = List.map split doc_ids in
  match mode, doc_ids with
  | `Index _, [] ->
    let%lwt () = Lwt_io.eprintl "only INDEX is provided and no DOC_ID" in
    Lwt.return `None
  | `Index index, [ doc_id; ] ->
    let%lwt (index, doc_type, doc_id) = map_doc_id' index doc_type doc_id in
    let doc_type = Option.default default_get_doc_type doc_type in
    Lwt.return (`Single (Some index, Some doc_type, Some doc_id))
  | `Index index, doc_id :: doc_ids ->
    let%lwt doc_id = map_doc_id' index doc_type doc_id in
    let%lwt doc_ids = Lwt_list.map_s (map_doc_id' index doc_type) doc_ids in
    multiple doc_id doc_ids
  | `IndexOrID (index, doc_id), [] ->
    let doc_type = Option.default default_get_doc_type doc_type in
    Lwt.return (`Single (Some index, Some doc_type, Some doc_id))
  | `IndexOrID (index, doc_type), doc_id :: doc_ids when List.for_all is_pure_id' (doc_id :: doc_ids) ->
    begin match doc_ids with
    | [] ->
      let%lwt (index, doc_type, doc_id) = map_typed_doc_id' index doc_type doc_id in
      Lwt.return (`Single (Some index, doc_type, Some doc_id))
    | _ ->
      let%lwt doc_id = map_typed_doc_id' index doc_type doc_id in
      let%lwt doc_ids = Lwt_list.map_s (map_typed_doc_id' index doc_type) doc_ids in
      multiple doc_id doc_ids
    end
  | `IndexOrID (index, doc_id), doc_ids ->
    let%lwt doc_ids = Lwt_list.map_s (map_index_doc_id' doc_type) doc_ids in
    multiple (index, doc_type, doc_id) doc_ids
  | `ID (index, doc_type, doc_id), [] ->
    Lwt.return (`Single (Some index, Some doc_type, Some doc_id))
  | `ID (index, doc_type', doc_id), doc_ids ->
    let%lwt doc_ids = Lwt_list.map_s (map_index_doc_id' doc_type) doc_ids in
    multiple (index, Some doc_type', doc_id) doc_ids

module Common_args = struct

  open Cmdliner

  let host = Arg.(required & pos 0 (some string) None & info [] ~docv:"HOST" ~doc:"host")

  let index = Arg.(required & pos 1 (some string) None & info [] ~docv:"INDEX" ~doc:"index")

  let doc_type = Arg.(value & opt (some string) None & info [ "T"; "doctype"; ] ~docv:"DOC_TYPE" ~doc:"document type")

  let doc_id = Arg.(pos 2 (some string) None & info [] ~docv:"DOC_ID" ~doc:"document id")

  let doc_ids =
    let doc = "document ids" in
    Arg.(value & pos_right 1 string [] & info [] ~docv:"DOC_ID1[ DOC_ID2[ DOC_ID3...]]" ~doc)

  let timeout = Arg.(value & opt (some string) None & info [ "t"; "timeout"; ] ~doc:"timeout")

  let source_includes = Arg.(value & opt_all string [] & info [ "i"; "source-includes"; ] ~doc:"source_includes")

  let source_excludes = Arg.(value & opt_all string [] & info [ "e"; "source-excludes"; ] ~doc:"source_excludes")

  let routing = Arg.(value & opt (some string) None & info [ "r"; "routing"; ] ~doc:"routing")

  let preference = Arg.(value & opt_all string [] & info [ "p"; "preference"; ] ~doc:"preference")

  let format =
    let parse format =
      match hit_format_of_string format with
      | exception Failure msg -> Error (`Msg msg)
      | format -> Ok format
    in
    let print fmt format =
      Format.fprintf fmt "%s" (string_of_hit_format format)
    in
    Arg.(list (conv (parse, print)))

  let format = Arg.(value & opt_all format [] & info [ "f"; "format"; ] ~doc:"map hits according to specified format (hit|id|source)")

end (* Common_args *)

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
  let (action, body) =
    match actions with
    | [] -> `GET, None
    | actions ->
    let actions = List.map (fun { action; index; alias; } -> [ action, { Elastic_t.index; alias; }; ]) actions in
    `POST, Some (JSON (Elastic_j.string_of_aliases { Elastic_t.actions; }))
  in
  Lwt_main.run @@
  match%lwt request ~verbose ?body action host [ Some "_aliases"; ] [] id with
  | Error error -> fail_lwt "alias error:\n%s" error
  | Ok result -> Lwt_io.printl result

type count_args = {
  host : string;
  index : string;
  doc_type : string option;
  timeout : string option;
  routing : string option;
  preference : string list;
  query : string option;
  body_query : string option;
  analyzer : string option;
  analyze_wildcard : bool;
  default_field : string option;
  default_operator : string option;
  retry : bool;
}

let count ({ verbose; _ } as _common_args) {
    host;
    index;
    doc_type;
    timeout;
    routing;
    preference;
    query;
    body_query;
    analyzer;
    analyze_wildcard;
    default_field;
    default_operator;
    retry = _;
  } =
  let config = Common.load_config () in
  let { Common.host; _ } = Common.get_cluster config host in
  Lwt_main.run @@
  let body_query = Option.map get_body_query_file body_query in
  let args = [
    "timeout", timeout;
    "routing", routing;
    "preference", csv ~sep:"|" preference;
    "analyzer", analyzer;
    "analyze_wildcard", flag analyze_wildcard;
    "df", default_field;
    "default_operator", default_operator;
    "q", query;
  ] in
  let body_query = match body_query with Some query -> Some (JSON query) | None -> None in
  let count () =
    match%lwt request ~verbose ?body:body_query `POST host [ Some index; doc_type; Some "_count"; ] args id with
    | Error error -> fail_lwt "count error:\n%s" error
    | Ok result ->
    Lwt_io.printl result
  in
  count ()

type delete_args = {
  host : string;
  index : string;
  doc_type : string option;
  doc_ids : string list;
  timeout : string option;
  routing : string option;
}

let delete ({ verbose; es_version; _ } as common_args) {
    host;
    index;
    doc_type;
    doc_ids;
    timeout;
    routing;
  } =
  let config = Common.load_config () in
  let { Common.host; version; _ } = Common.get_cluster config host in
  Lwt_main.run @@
  let%lwt ({ default_get_doc_type; _ }) =
    get_es_version_config common_args host es_version config version
  in
  match%lwt map_ids ~default_get_doc_type index doc_type doc_ids with
  | `None -> Lwt.return_unit
  | `Single _ | `Multi _ as mode ->
  let (action, body, path) =
    match mode with
    | `Single (index, doc_type, doc_id) -> `DELETE, None, [ index; doc_type; doc_id; ]
    | `Multi (docs, index, doc_type) ->
    let body =
      List.fold_left begin fun acc (index, doc_type, doc_id) ->
        let delete = { Elastic_t.index; doc_type; id = doc_id; routing = None; } in
        let bulk = { Elastic_t.index = None; create = None; update = None; delete = Some delete; } in
        "\n" :: Elastic_j.string_of_bulk bulk :: acc
      end [] docs |>
      List.rev |>
      String.concat ""
    in
    `POST, Some (NDJSON body), [ index; doc_type; Some "_bulk"; ]
  in
  let args = [
    "timeout", timeout;
    "routing", routing;
  ] in
  match%lwt request ~verbose ?body action host path args id with
  | Error response -> Lwt_io.eprintl response
  | Ok response -> Lwt_io.printl response

type flush_args = {
  host : string;
  indices : string list;
  force : bool;
  synced : bool;
  wait : bool;
}

let flush { verbose; _ } {
    host;
    indices;
    force;
    synced;
    wait;
  } =
  let config = Common.load_config () in
  let { Common.host; _ } = Common.get_cluster config host in
  let bool' v = function true -> Some v | false -> None in
  let bool = bool' "true" in
  let args = [
    "force", bool force;
    "wait_if_ongoing", bool wait;
  ] in
  let path = [ csv indices; Some "_flush"; bool' "synced" synced; ] in
  Lwt_main.run @@
  match%lwt request ~verbose `POST host path args id with
  | Error error -> fail_lwt "flush error:\n%s" error
  | Ok result -> Lwt_io.printl result

type get_args = {
  host : string;
  index : string;
  doc_type : string option;
  doc_ids : string list;
  timeout : string option;
  source_includes : string list;
  source_excludes : string list;
  routing : string option;
  preference : string list;
  format : hit_format list list;
}

let get ({ verbose; es_version; _ } as common_args) {
    host;
    index;
    doc_type;
    doc_ids;
    timeout;
    source_includes;
    source_excludes;
    routing;
    preference;
    format;
  } =
  let config = Common.load_config () in
  let { Common.host; version; _ } = Common.get_cluster config host in
  Lwt_main.run @@
  let%lwt ({ default_get_doc_type; _ }) =
    get_es_version_config common_args host es_version config version
  in
  match%lwt map_ids ~default_get_doc_type index doc_type doc_ids with
  | `None -> Lwt.return_unit
  | `Single _ | `Multi _ as mode ->
  let (body, path, unformat) =
    match mode with
    | `Single (index, doc_type, doc_id) ->
      let path = [ index; doc_type; doc_id; ] in
      let unformat x = [ Elastic_j.option_hit_of_string J.read_json x; ] in
      None, path, unformat
    | `Multi (docs, index, doc_type) ->
    let (docs, ids) =
      match index, doc_type with
      | Some _, Some _ ->
        let ids = List.map (fun (_index, _doc_type, doc_id) -> doc_id) docs in
        [], ids
      | _ ->
      let docs =
        List.map begin fun (index, doc_type, id) ->
          { Elastic_t.index; doc_type; id; routing = None; source = None; stored_fields = None; }
        end docs
      in
      docs, []
    in
    let path = [ index; doc_type; Some "_mget"; ] in
    let unformat x =
      let { Elastic_t.docs; } = Elastic_j.docs_of_string (Elastic_j.read_option_hit J.read_json) x in
      docs
    in
    Some (JSON (Elastic_j.string_of_multiget { docs; ids; })), path, unformat
  in
  let args = [
    "timeout", timeout;
    (if source_excludes = [] then "_source" else "_source_includes"), csv source_includes;
    "_source_excludes", csv source_excludes;
    "routing", routing;
    "preference", csv ~sep:"|" preference;
  ] in
  let request unformat = request ~verbose ?body `GET host path args unformat in
  match format with
  | [] ->
    begin match%lwt request id with
    | Error response -> Lwt_io.eprintl response
    | Ok response -> Lwt_io.printl response
    end
  | _ ->
  match%lwt request unformat with
  | Error response -> Lwt_io.eprintl response
  | Ok docs ->
  Lwt_list.iter_s begin fun hit ->
    List.map (List.map map_of_hit_format) format |>
    List.concat |>
    List.map (fun f -> f hit) |>
    String.join " " |>
    Lwt_io.printl
  end docs

type health_args = {
  hosts : string list;
}

let health { verbose; _ } {
    hosts;
  } =
  let config = Common.load_config () in
  let all_hosts = lazy (List.map (fun (name, _) -> Common.get_cluster config name) config.Config_t.clusters) in
  let hosts =
    match List.rev hosts with
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
      let args = [ "h", Some (String.concat "," columns); ] in
      match%lwt request ~verbose `GET host [ Some "_cat"; Some "health"; ] args id with
      | Error error -> Lwt.return (i, sprintf "%s error %s\n" host error)
      | Ok result -> Lwt.return (i, sprintf "%s %s" host result)
    end hosts
  in
  List.sort ~cmp:(Factor.Int.compare $$ fst) results |>
  Lwt_list.iter_s (fun (_i, result) -> Lwt_io.print result)

type nodes_args = {
  host : string;
  check_nodes : string list;
}

let nodes { verbose; _ } {
    host;
    check_nodes;
  } =
  let config = Common.load_config () in
  let { Common.host; nodes; _ } = Common.get_cluster config host in
  let check_nodes = match check_nodes with [] -> Option.default [] nodes | nodes -> nodes in
  let check_nodes = SS.of_list (List.concat (List.map Common.expand_node check_nodes)) in
  Lwt_main.run @@
  match%lwt request ~verbose `GET host [ Some "_nodes"; ] [] J.from_string with
  | Error error -> fail_lwt "nodes error:\n%s" error
  | Ok result ->
  J.Util.member "nodes" result |>
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

type put_args = {
  host : string;
  index : string;
  doc_type : string option;
  doc_id : string option;
  routing : string option;
  body : string option;
}

let put ({ verbose; es_version; _ } as common_args) {
    host;
    index;
    doc_type;
    doc_id;
    routing;
    body;
  } =
  let config = Common.load_config () in
  let { Common.host; version; _ } = Common.get_cluster config host in
  Lwt_main.run @@
  let%lwt { default_put_doc_type; _ } =
    get_es_version_config common_args host es_version config version
  in
  let%lwt (index, doc_type, doc_id) =
    let%lwt mode = Lwt.wrap1 map_index_mode index in
    match mode, doc_id with
    | `Index index, None -> Lwt.return (index, doc_type, None)
    | `Index index, Some doc_id -> map_doc_id_opt index doc_type doc_id
    | `IndexOrID (index, doc_id), None -> Lwt.return (index, doc_type, Some doc_id)
    | `IndexOrID (index, doc_type), Some doc_id -> map_typed_doc_id_opt index doc_type doc_id
    | `ID (index, doc_type, doc_id), None -> Lwt.return (index, Some doc_type, Some doc_id)
    | `ID (index, doc_type, doc_id1), Some doc_id2 ->
      Exn_lwt.fail "invalid document id : /%s/%s/%s/%s" index doc_type doc_id1 doc_id2
  in
  let%lwt doc_type =
    match coalesce [ doc_type; default_put_doc_type; ] with
    | Some doc_type -> Lwt.return doc_type
    | None -> Exn_lwt.fail "DOC_TYPE is not provided"
  in
  let args = [ "routing", routing; ] in
  let%lwt body = match body with Some body -> Lwt.return body | None -> Lwt_io.read Lwt_io.stdin in
  let action = if doc_id <> None then `PUT else `POST in
  match%lwt request ~verbose ~body:(JSON body) action host [ Some index; Some doc_type; doc_id; ] args id with
  | Error error -> fail_lwt "put error:\n%s" error
  | Ok result -> Lwt_io.printl result

type recovery_args = {
  host : string;
  indices : string list;
  filter_include : (index_shard_format * string) list;
  filter_exclude : (index_shard_format * string) list;
  format : index_shard_format list list;
}

let recovery { verbose; _ } {
    host;
    indices;
    filter_include;
    filter_exclude;
    format;
  } =
  let format =
    match format with
    | [] -> List.map map_of_index_shard_format default_index_shard_format
    | _ ->
    List.map (List.map map_of_index_shard_format) format |>
    List.concat
  in
  let config = Common.load_config () in
  let filter_include = List.map (fun (k, v) -> map_of_index_shard_format k, v) filter_include in
  let filter_exclude = List.map (fun (k, v) -> map_of_index_shard_format k, v) filter_exclude in
  let { Common.host; _ } = Common.get_cluster config host in
  Lwt_main.run @@
  match%lwt request ~verbose `GET host [ csv indices; Some "_recovery"; ] [] Elastic_j.indices_shards_of_string with
  | Error error -> fail_lwt "recovery error:\n%s" error
  | Ok indices ->
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

type refresh_args = {
  host : string;
  indices : string list;
}

let refresh { verbose; _ } {
    host;
    indices;
  } =
  let config = Common.load_config () in
  let { Common.host; _ } = Common.get_cluster config host in
  Lwt_main.run @@
  match%lwt request ~verbose `POST host [ csv indices; Some "_refresh"; ] [] id with
  | Error error -> fail_lwt "refresh error:\n%s" error
  | Ok result -> Lwt_io.printl result

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
  routing : string option;
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
  format : hit_format list list;
}

let search ({ verbose; es_version; _ } as common_args) {
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
  Lwt_main.run @@
  let%lwt { read_total; write_total; _ } =
    get_es_version_config common_args host es_version config version
  in
  let body_query = Option.map get_body_query_file body_query in
  let args = [
    "timeout", timeout;
    "size", int size;
    "from", int from;
    "track_total_hits", track_total_hits;
    "sort", csv sort;
    (if source_excludes = [] then "_source" else "_source_includes"), csv source_includes;
    "_source_excludes", csv source_excludes;
    "stored_fields", csv fields;
    "routing", routing;
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
    List.map (List.map map_of_hit_format) format |>
    List.concat
  in
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
  let body_query = match body_query with Some query -> Some (JSON query) | None -> None in
  let htbl = Hashtbl.create (if retry then Option.default 10 size else 0) in
  let rec search () =
    match%lwt request ~verbose ?body:body_query `POST host [ Some index; doc_type; Some "_search"; ] args id with
    | Error error -> fail_lwt "search error:\n%s" error
    | Ok result ->
    match show_count, format, scroll, retry with
    | false, [], None, false -> Lwt_io.printl result
    | show_count, format, scroll, retry ->
    let scroll_path = [ Some "_search"; Some "scroll"; ] in
    let clear_scroll' scroll_id =
      let clear_scroll = JSON (Elastic_j.string_of_clear_scroll { Elastic_t.scroll_id = [ scroll_id; ]; }) in
      match%lwt request ~verbose ~body:clear_scroll `DELETE host scroll_path [] id with
      | Error error -> fail_lwt "clear scroll error:\n%s" error
      | Ok _ok -> Lwt.return_unit
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
      let scroll = JSON (Elastic_j.string_of_scroll { Elastic_t.scroll; scroll_id; }) in
      match%lwt request ~verbose ~body:scroll `POST host scroll_path [] id with
      | Error error ->
        let%lwt () = Lwt_io.eprintlf "scroll error:\n%s" error in
        let%lwt () = clear_scroll' scroll_id in
        Lwt.fail ErrorExit
      | Ok result -> loop result
    in
    loop result
  in
  search ()

module Cluster_settings = struct

  type input =
    | Text
    | JSON

  type output =
    | Text
    | JSON
    | Raw

  type type_ =
    | Transient
    | Persistent
    | Defaults

  type args = {
    host : string;
    keys : string list;
    reset : bool;
    include_defaults : bool;
    input : input;
    output : output;
    type_ : type_ option;
  }

  let cluster_settings { verbose; _ } {
      host;
      keys;
      reset;
      include_defaults;
      input;
      output;
      type_;
    } =
    let config = Common.load_config () in
    let { Common.host; _ } = Common.get_cluster config host in
    let path = [ Some "_cluster"; Some "settings"; ] in
    let (get_keys, set_keys) =
      List.map begin fun s ->
        match Stre.splitc s '=' with
        | exception Not_found when reset -> `Set (s, None)
        | exception Not_found -> `Get s
        | key, value -> `Set (key, Some value)
      end keys |>
      List.partition (function `Get _ -> true | `Set _ -> false)
    in
    let get_keys = List.map (function `Get key -> key | `Set _ -> assert false) get_keys in
    let set_keys = List.map (function `Get _ -> assert false | `Set pair -> pair) set_keys in
    Lwt_main.run @@
    let%lwt set_mode =
      match set_keys, type_ with
      | [], _ -> Lwt.return_none
      | _, Some Transient -> Lwt.return_some `Transient
      | _, Some Persistent -> Lwt.return_some `Persistent
      | _, Some Defaults -> fail_lwt "type defaults is not valid for set or reset" (* FIXME *)
      | _, None -> fail_lwt "type is missing" (* FIXME *)
    in
    let%lwt () =
      match set_mode with
      | None -> Lwt.return_unit
      | Some mode ->
      let%lwt values =
        Lwt_list.map_s begin fun (key, value) ->
          let%lwt value =
            match value with
            | None -> Lwt.return `Null
            | Some value ->
            match input with
            | Text -> Lwt.return (`String value)
            | JSON -> Lwt.wrap1 J.from_string value
          in
          Lwt.return (key, value)
        end set_keys
      in
      let values = Some (`Assoc values) in
      let (transient, persistent) =
        match mode with
        | `Transient -> values, None
        | `Persistent -> None, values
      in
      let settings = ({ transient; persistent; defaults = None; } : Elastic_t.cluster_tree_settings) in
      let body = (JSON (Elastic_j.string_of_cluster_tree_settings settings) : content_type) in
      match%lwt request ~verbose ~body `PUT host path [] id with
      | Error error -> fail_lwt "settings error:\n%s" error
      | Ok result -> Lwt_io.printl result
    in
    let%lwt () =
      match get_keys, set_keys with
      | [], _ :: _ -> Lwt.return_unit
      | _ ->
      let include_defaults = include_defaults || type_ = Some Defaults in
      let args = [
        "flat_settings", Some "true";
        "include_defaults", flag include_defaults;
      ] in
      match%lwt request ~verbose `GET host path args id with
      | Error error -> fail_lwt "settings error:\n%s" error
      | Ok result ->
      match get_keys, output, type_ with
      | [], Raw, None -> Lwt_io.printl result
      | _ ->
      let%lwt { Elastic_t.transient; persistent; defaults; } = Lwt.wrap1 Elastic_j.cluster_flat_settings_of_string result in
      let type_settings =
        match type_ with
        | None -> None
        | Some Defaults -> Some defaults
        | Some Transient -> Some transient
        | Some Persistent -> Some persistent
      in
      let output =
        match output with
        | Text -> `Text
        | JSON -> `JSON
        | Raw -> `Raw
      in
      let module SS = Set.Make(String) in
      let get_keys = SS.of_list get_keys in
      let get_keys_empty = SS.is_empty get_keys in
      let get_keys_typed_single = SS.cardinal get_keys = 1 && Option.is_some type_ in
      match output with
      | `Raw ->
        let filter =
          match get_keys_empty with
          | true -> id
          | false -> (fun settings -> List.filter (fun (key, _value) -> SS.mem key get_keys) settings)
        in
        begin match type_settings with
        | Some settings ->
          let settings = Option.map_default filter [] settings in
          Lwt_io.printl (Elastic_j.string_of_settings settings)
        | None ->
          let transient = Option.map filter transient in
          let persistent = Option.map filter persistent in
          let defaults = Option.map filter defaults in
          Lwt_io.printl (Elastic_j.string_of_cluster_flat_settings { Elastic_t.transient; persistent; defaults; })
        end
      | `Text | `JSON as output ->
      let settings =
        match type_settings with
        | Some settings -> [ None, settings; ]
        | None -> [ Some "transient: ", transient; Some "persistent: ", persistent; Some "defaults: ", defaults; ]
      in
      let string_of_value =
        match output with
        | `JSON -> (fun value -> J.to_string value)
        | `Text ->
        function
        | `Null -> "null"
        | `Intlit s | `String s -> s
        | `Bool x -> string_of_bool x
        | `Int x -> string_of_int x
        | `Float x -> string_of_float x
        | `List _ | `Assoc _ | `Tuple _ | `Variant _ as value -> J.to_string value
      in
      let print_value value =
        Lwt_io.printl (string_of_value value)
      in
      let print_key_value prefix key value =
        Lwt_io.printlf "%s%s: %s" prefix key (string_of_value value)
      in
      let print prefix (key, value) =
        match prefix, get_keys_typed_single with
        | None, true -> print_value value
        | _ -> print_key_value (Option.default "" prefix) key value
      in
      Lwt_list.iter_s begin function
        | _prefix, None -> Lwt.return_unit
        | prefix, Some settings ->
        match get_keys_empty with
        | true -> Lwt_list.iter_s (print prefix) settings
        | _ ->
        Lwt_list.iter_s (function key, _ as pair when SS.mem key get_keys -> print prefix pair | _ -> Lwt.return_unit) settings
      end settings
    in
    Lwt.return_unit

end (* Cluster_settings *)

open Cmdliner

module Let_syntax = struct

  let map ~f t = Term.(const f $ t)

  let both a b = Term.(const (fun x y -> x, y) $ a $ b)

end

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
  let open Common_args in
  let%map common_args = common_args
  and host = host
  and index =
    let doc = "index to operate on. If not provided, -a and -r must include the =INDEX part." in
    Arg.(value & pos 1 (some string) None & info [] ~docv:"INDEX" ~doc)
  and add =
    let doc = "add index INDEX to alias ALIAS" in
    Arg.(value & opt_all action [] & info [ "a"; "add"; ] ~docv:"ALIAS[=INDEX]" ~doc)
  and remove =
    let doc = "remove index INDEX from alias ALIAS" in
    Arg.(value & opt_all action [] & info [ "r"; "remove"; ] ~docv:"ALIAS[=INDEX]" ~doc)
  in
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

let alias_tool =
  alias_tool,
  let open Term in
  let doc = "add or remove index aliases" in
  let exits = default_exits in
  let man = [] in
  info "alias" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let count_tool =
  let open Common_args in
  let%map common_args = common_args
  and host = host
  and index = index
  and doc_type = doc_type
  and timeout = timeout
  and routing = routing
  and preference = preference
  and query = Arg.(value & opt (some string) None & info [ "q"; "query"; ] ~doc:"query using query_string query")
  and body_query = Arg.(value & pos 2 (some string) None & info [] ~docv:"BODY_QUERY" ~doc:"body query")
  and analyzer = Arg.(value & opt (some string) None & info [ "a"; "analyzer"; ] ~doc:"analyzer to be used for query_string query")
  and analyze_wildcard = Arg.(value & flag & info [ "w"; "analyze-wildcard"; ] ~doc:"analyze wildcard and prefix queries in query_string query")
  and default_field = Arg.(value & opt (some string) None & info [ "d"; "default-field"; ] ~doc:"default field to be used for query_string query")
  and default_operator = Arg.(value & opt (some string) None & info [ "O"; "default-operator"; ] ~doc:"default operator to be used for query_string query")
  and retry = Arg.(value & flag & info [ "R"; "retry"; ] ~doc:"retry if there are any failed shards") in
  count common_args {
    host;
    index;
    doc_type;
    timeout;
    routing;
    preference;
    query;
    body_query;
    analyzer;
    analyze_wildcard;
    default_field;
    default_operator;
    retry;
  }

let count_tool =
  count_tool,
  let open Term in
  let doc = "count" in
  let exits = default_exits in
  let man = [] in
  info "count" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let delete_tool =
  let open Common_args in
  let%map common_args = common_args
  and host = host
  and index = index
  and doc_type = doc_type
  and doc_ids = doc_ids
  and timeout = timeout
  and routing = routing in
  delete common_args {
    host;
    index;
    doc_type;
    doc_ids;
    timeout;
    routing;
  }

let delete_tool =
  delete_tool,
  let open Term in
  let doc = "delete document(s)" in
  let exits = default_exits in
  let man = [] in
  info "delete" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let flush_tool =
  let open Common_args in
  let%map common_args = common_args
  and host = host
  and indices =
    let doc = "indices to flush" in
    Arg.(value & pos_right 0 string [] & info [] ~docv:"INDEX1[ INDEX2[ INDEX3...]]" ~doc)
  and force = Arg.(value & flag & info [ "f"; "force"; ] ~doc:"force flush")
  and synced = Arg.(value & flag & info [ "s"; "synced"; ] ~doc:"synced flush")
  and wait = Arg.(value & flag & info [ "w"; "wait"; ] ~doc:"wait if another flush is already ongoing") in
  flush common_args {
    host;
    indices;
    force;
    synced;
    wait;
  }

let flush_tool =
  flush_tool,
  let open Term in
  let doc = "flush indices" in
  let exits = default_exits in
  let man = [] in
  info "flush" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let get_tool =
  let open Common_args in
  let%map common_args = common_args
  and host = host
  and index = index
  and doc_type = doc_type
  and doc_ids = doc_ids
  and timeout = timeout
  and source_includes = source_includes
  and source_excludes = source_excludes
  and routing = routing
  and preference = preference
  and format = format in
  get common_args {
    host;
    index;
    doc_type;
    doc_ids;
    timeout;
    source_includes;
    source_excludes;
    routing;
    preference;
    format;
  }

let get_tool =
  get_tool,
  let open Term in
  let doc = "get document(s)" in
  let exits = default_exits in
  let man = [] in
  info "get" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let health_tool =
  let%map common_args = common_args
  and hosts = Arg.(value & pos_all string [] & info [] ~docv:"HOST1[ HOST2[ HOST3...]]" ~doc:"hosts") in
  health common_args {
    hosts;
  }

let health_tool =
  health_tool,
  let open Term in
  let doc = "cluster health" in
  let exits = default_exits in
  let man = [] in
  info "health" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let nodes_tool =
  let open Common_args in
  let%map common_args = common_args
  and host = host
  and check_nodes =
    let doc = "check presence of specified nodes" in
    Arg.(value & pos_right 0 string [] & info [] ~docv:"HOST1[ HOST2[ HOST3...]]" ~doc)
  in
  nodes common_args {
    host;
    check_nodes;
  }

let nodes_tool =
  nodes_tool,
  let open Term in
  let doc = "cluster nodes" in
  let exits = default_exits in
  let man = [] in
  info "nodes" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let put_tool =
  let open Common_args in
  let%map common_args = common_args
  and host = host
  and index = index
  and doc_type = doc_type
  and doc_id = Arg.value doc_id
  and routing = routing
  and body =
    let doc = "document source to put" in
    Arg.(value & opt (some string) None & info [ "s"; "source"; ] ~docv:"DOC" ~doc)
  in
  put common_args {
    host;
    index;
    doc_type;
    doc_id;
    routing;
    body;
  }

let put_tool =
  put_tool,
  let open Term in
  let doc = "put document" in
  let exits = default_exits in
  let man = [] in
  info "put" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let recovery_tool =
  let format =
    let parse format =
      match index_shard_format_of_string format with
      | exception Failure msg -> Error (`Msg msg)
      | format -> Ok format
    in
    let print fmt format =
      Format.fprintf fmt "%s" (string_of_index_shard_format format)
    in
    Arg.conv (parse, print)
  in
  let filter = Arg.pair ~sep:'=' format Arg.string in
  let format = Arg.list format in
  let%map common_args = common_args
  and host = Common_args.host
  and indices =
    let doc = "indices to check" in
    Arg.(value & pos_right 0 string [] & info [] ~docv:"INDEX1[ INDEX2[ INDEX3...]]" ~doc)
  and format = Arg.(value & opt_all format [] & info [ "f"; "format"; ] ~doc:"map hits according to specified format")
  and filter_include =
    let doc = "include only shards matching filter" in
    Arg.(value & opt_all filter [] & info [ "i"; "include"; ] ~doc ~docv:"COLUMN=VALUE")
  and filter_exclude =
    let doc = "exclude shards matching filter" in
    Arg.(value & opt_all filter [] & info [ "e"; "exclude"; ] ~doc ~docv:"COLUMN=VALUE")
  in
  recovery common_args {
    host;
    indices;
    filter_include;
    filter_exclude;
    format;
  }

let recovery_tool =
  recovery_tool,
  let open Term in
  let doc = "cluster recovery" in
  let exits = default_exits in
  let man = [] in
  info "recovery" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let refresh_tool =
  let open Common_args in
  let%map common_args = common_args
  and host = host
  and indices =
    let doc = "indices to refresh" in
    Arg.(value & pos_right 0 string [] & info [] ~docv:"INDEX1[ INDEX2[ INDEX3...]]" ~doc)
  in
  refresh common_args {
    host;
    indices;
  }

let refresh_tool =
  refresh_tool,
  let open Term in
  let doc = "refresh indices" in
  let exits = default_exits in
  let man = [] in
  info "refresh" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let search_tool =
  let open Common_args in
  let%map common_args = common_args
  and host = host
  and index = index
  and doc_type = doc_type
  and timeout = timeout
  and source_includes = source_includes
  and source_excludes = source_excludes
  and routing = routing
  and preference = preference
  and format = format
  and size = Arg.(value & opt (some int) None & info [ "n"; "size"; ] ~doc:"size")
  and from = Arg.(value & opt (some int) None & info [ "o"; "from"; ] ~doc:"from")
  and sort = Arg.(value & opt_all string [] & info [ "s"; "sort"; ] ~doc:"sort")
  and fields = Arg.(value & opt_all string [] & info [ "F"; "fields"; ] ~doc:"fields")
  and scroll = Arg.(value & opt (some string) None & info [ "S"; "scroll"; ] ~doc:"scroll")
  and slice_max = Arg.(value & opt (some int) None & info [ "N"; "slice-max"; ] ~doc:"slice_max")
  and slice_id = Arg.(value & opt (some int) None & info [ "I"; "slice-id"; ] ~doc:"slice_id")
  and query = Arg.(value & opt (some string) None & info [ "q"; "query"; ] ~doc:"query using query_string query")
  and body_query = Arg.(value & pos 2 (some string) None & info [] ~docv:"BODY_QUERY" ~doc:"body query")
  and analyzer = Arg.(value & opt (some string) None & info [ "a"; "analyzer"; ] ~doc:"analyzer to be used for query_string query")
  and analyze_wildcard = Arg.(value & flag & info [ "w"; "analyze-wildcard"; ] ~doc:"analyze wildcard and prefix queries in query_string query")
  and default_field = Arg.(value & opt (some string) None & info [ "d"; "default-field"; ] ~doc:"default field to be used for query_string query")
  and default_operator = Arg.(value & opt (some string) None & info [ "O"; "default-operator"; ] ~doc:"default operator to be used for query_string query")
  and explain = Arg.(value & flag & info [ "E"; "explain"; ] ~doc:"explain hits")
  and show_count = Arg.(value & flag & info [ "c"; "show-count"; ] ~doc:"output total number of hits")
  and track_total_hits = Arg.(value & opt (some string) None & info [ "C"; "track-total-hits"; ] ~doc:"track total number hits (true, false, or a number)")
  and retry = Arg.(value & flag & info [ "R"; "retry"; ] ~doc:"retry if there are any failed shards") in
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

let search_tool =
  search_tool,
  let open Term in
  let doc = "search" in
  let exits = default_exits in
  let man = [] in
  info "search" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let cluster_settings_tool =
  let open Common_args in
  let open Cluster_settings in
  let%map common_args = common_args
  and host = host
  and keys = Arg.(value & pos_right 0 string [] & info [] ~docv:"KEYS" ~doc:"setting keys")
  and reset = Arg.(value & flag & info [ "r"; "reset"; ] ~doc:"reset keys")
  and include_defaults = Arg.(value & flag & info [ "D"; "include-defaults"; ] ~doc:"include defaults")
  and input = Arg.(value & vflag (Text : input) [ JSON, info [ "j"; "input-json"; ] ~doc:"json input format"; ])
  and output =
    let output =
      let parse output =
        match output with
        | "text" -> Ok (Text : output)
        | "json" -> Ok JSON
        | "raw" -> Ok Raw
        | _ -> Error (`Msg (sprintf "unknown output format: %s" output))
      in
      let print fmt output =
        let output =
          match output with
          | (Text : output) -> "text"
          | JSON -> "json"
          | Raw -> "raw"
        in
        Format.fprintf fmt "%s" output
      in
      Arg.(conv (parse, print))
    in
    Arg.(value & opt ~vopt:(JSON : output) output Text & info [ "J"; "output"; ] ~doc:"choose output format")
  and type_ = 
    let type_transient = Some Transient, Arg.info [ "t"; "transient"; ] ~doc:"transient setting" in
    let type_persistent = Some Persistent, Arg.info [ "p"; "persistent"; ] ~doc:"persistent setting" in
    let type_defaults = Some Defaults, Arg.info [ "d"; "default"; ] ~doc:"default setting" in
    Arg.(value & vflag None [ type_transient; type_persistent; type_defaults; ])
  in
  cluster_settings common_args {
    host;
    keys;
    reset;
    include_defaults;
    input;
    output;
    type_;
  }

let cluster_settings_tool =
  cluster_settings_tool,
  let open Term in
  let doc = "manage cluster settings" in
  let exits = default_exits in
  let man = [] in
  info "cluster_settings" ~doc ~sdocs:Manpage.s_common_options ~exits ~man

let tools = [
  alias_tool;
  count_tool;
  cluster_settings_tool;
  delete_tool;
  flush_tool;
  get_tool;
  health_tool;
  nodes_tool;
  put_tool;
  recovery_tool;
  refresh_tool;
  search_tool;
]

let () =
  try
    let argv = Common.get_argv () in
    Term.(exit (eval_choice ~catch:false ~argv default_tool tools))
  with
  | ErrorExit -> exit 1
  | exn -> log #error ~exn "uncaught exception"; exit 125
