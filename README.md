es-cli
======

This project provides a command line tool to query ElasticSearch clusters.

Configuration file
------------------

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

Examples
--------

Check health of multiple clusters:

```
es health cluster1.mydomain.com:9200 cluster2.mydomain.com:9200
```

Check nodes of a cluster (expect data0...data9, client0...client4 and master nodes to be present):

```
es nodes cluster1.mydomain.com:9200 -h data{0..9} master client{0..4}
```

Query/search documents in an index:

```
es search cluster1.mydomain.com:9200/myindex -i field1,field2 -s updated_at:desc -n 1 -q 'title:"Hello world!"'
```
