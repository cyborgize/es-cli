es-cli
======

This project provides a command line utility to query ElasticSearch clusters.

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
