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
es health cluster1 # show health for cluster1.mydomain.com:9200
es health # show health for all configured clusters
```

```
es search cluster2 myindex
```

## Examples

### Add or remove index alias

Add alias `alias1` to `myindex1` and alias `alias2` to `myindex2`:

```
es alias cluster1.mydomain.com:9200 -a alias1=myindex1 -a alias2=myindex2
```

Remove alias `alias1` from `myindex1` and alias `alias2` from `myindex2`:

```
es alias cluster1.mydomain.com:9200 -r alias1=myindex1 -r alias2=myindex2
```

Move index alias `current` from `index-3` to `index-4`
```
es alias cluster1.mydomain.com:9200 -r current=index-3 -a current=index-4
```

Remove alias `alias1` and add alias `alias2` to `index`.
```
es alias cluster1.mydomain.com:9200 index -r alias1 -a alias2
```

### Get document by id

```
es get cluster1.mydomain.com:9200 myindex docid
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

Expect all nodes listed for cluster `mycluster` in the `elasticsearch-cli` configuration to be present:

```
es nodes mycluster
```

### Put document with or without id

```
es put cluster1.mydomain.com:9200 myindex docid '{ "first_name": "John", "last_name": "Doe" }'
```

```
es put cluster1.mydomain.com:9200 myindex '{ "first_name": "Jane", "last_name": "Doe" }'
```

```
echo '{ "first_name": "Johnny", "last_name": "Doe" }' | es put cluster1.mydomain.com:9200 myindex docid2
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

Search the index `myindex` for documents containing `"Hello world!"` in the `title` field. Return fields
`field1` and `field2` of the document with the most recent value of the `updated_at` field:

```
es search cluster1.mydomain.com:9200 myindex -i field1,field2 -s updated_at:desc -n 1 -q 'title:"Hello world!"'
```

Search the index `myindex` for documents containing `12345` in the `field1` field. Return 10 documents' sources,
omitting the `boringfield` field.

```
es search cluster1.mydomain.com:9200 myindex -e boringfield -n 10 -f source '{"query":{"term":{"field1":12345}}}'
```

Show the number of documents in the index `myindex` with field `field1` value greater or equal to 10:

```
es search cluster1.mydomain.com:9200 myindex -n 0 -c -q 'field1:>=10'
```
