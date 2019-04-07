# Change Log

## 0.5
- build: switch build system to dune
- search: use `-F` to include stored fields
- search: use `-R` to retry search if there are failed shards
- search: use `-E` to explain hits
- search: use `-a`, `-d`, `-O` to set analyzer, default field and default operator for `-q`
- search: use `-w` to analyze wildcard and prefix queries

## 0.4
- use `-t` to set search timeout
- use `-T` to set HTTP request timeout
- fix dependency on lwt ppx

## 0.3
- `flush` tool
- sliced scroll (`-N` and `-I`)
- use `-S` for scroll search instead of `-scroll`
- fix args after `--` being ignored

## 0.2
- `put`, `recovery` and `refresh` tools
- split output format (`-f`) by comma
- fix source\_exclude (`-e`) and preference (`-p`)

## 0.1
- `alias`, `get`, `health`, `nodes` and `search` tools
