# elasticsearch-cli — Command-line client for Elasticsearch

This project provides a command line tool to query ElasticSearch clusters.

## Installation
elasticsearch-cli can be installed with `opam`:

    opam install elasticsearch-cli

## Configuration file

The tool will look for a configuration file `$XDG_HOME_CONFIG/es-cli/config.json` when started
(`$XDG_HOME_CONFIG` will be usually `~/.config`; see [XDG Base Directory Specification](https://specifications.freedesktop.org/basedir-spec/basedir-spec-0.6.html) for more details).

An example configuration file:

```
{
  "clusters": {
    "cluster1": {
      "host": "cluster1.mydomain.com:9200"
    },
    "cluster2": {
      "host": "cluster2.mydomain.com:9200",
      "nodes": [
        "master",
        "data{0..9}",
        "client{0..4}"
      ]
    }
  }
}
```

With the above configuration file, it is possible to use alias names instead of full host names, for example:

```
es health cluster1 # will show health for cluster1.mydomain.com:9200
es health # will show health for all configured clusters
```

```
es search cluster2/myindex
```

## Examples

### Add or remove index alias

```
es alias cluster1.mydomain.com:9200 -a myindex1 alias1 -a myindex2 alias2
```

```
es alias cluster1.mydomain.com:9200 -r myindex1 alias1 -r myindex2 alias2
```

### Get document by id

```
es get cluster1.mydomain.com:9200/myindex/doctype/docid
```

### Check health of multiple clusters

```
es health cluster1.mydomain.com:9200 cluster2.mydomain.com:9200
```

### Check nodes of a cluster

Expect data0...data9, client0...client4 and master nodes to be present):

```
es nodes cluster1.mydomain.com:9200 -h data{0..9} master client{0..4}
```

### Check shard recovery status

Display shards which are not in `DONE` stage:
```
es recovery cluster1.mydomain.com:9200 -e stage done
```

### Refresh indices

```
es refresh cluster1.mydomain.com:9200 myindex1 myindex2
```

### Query/search documents in an index

```
es search cluster1.mydomain.com:9200 myindex -i field1,field2 -s updated_at:desc -n 1 -q 'title:"Hello world!"'
```
