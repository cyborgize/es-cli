open Prelude
open ExtLib
open Printf

let log = Log.from "es"

let usage tools =
  fprintf stderr "Usage: %s {<tool>|-help|version}\n" Sys.executable_name;
  fprintf stderr "where <tool> is one of:\n";
  List.sort ~cmp:compare tools |>
  List.iter (fun (s,_) -> fprintf stderr "  %s\n" s)

let search () =
  let cmd = ref [] in
  let size = ref None in
  let from = ref None in
  let sort = ref [] in
  let source_include = ref [] in
  let source_exclude = ref [] in
  let args = ExtArg.[
    may_int "n" size "<n> #set search limit";
    may_int "o" from "<n> #set search offset";
    "-s", String (tuck sort), "<field[:dir]> #set sort order";
    "-i", String (tuck source_include), "<field> #include source field";
    "-e", String (tuck source_exclude), "<field> #exclude source field";
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
  | `Ok result -> Lwt_io.printl result

let () =
  let tools = [
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
