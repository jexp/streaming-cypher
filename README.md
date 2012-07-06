## Streaming Cypher

Just put the [jar](https://github.com/downloads/neo4j-contrib/streaming-cypher/streaming-cypher-extension-1.7.M03.jar) (streaming-cypher-extension-1.7.M03.jar) into neo4j-server/plugins and add this to the conf/neo4j-server.properties file

    org.neo4j.server.thirdparty_jaxrs_classes=org.neo4j.server.extension.streaming.cypher=/streaming

Then you can issue

    curl -d'{"query":"start n=node(*) return n"}' -H accept:application/json -H content-type:application/json http://localhost:7474/streaming/cypher

to query the graph and stream the results.

Note, for rendering the existing format of the Neo4j-REST API (without the additional discoverable URLs) use `mode=compat`:

    curl -d'{"query":"start n=node(*) return n"}' -H accept:application/json;mode=compat -H content-type:application/json http://localhost:7474/streaming/cypher

A pretty printing result is acquired by adding `format=pretty to the Accept Header.

    curl -d'{"query":"start n=node(*) return n"}' -H accept:application/json;format=pretty -H content-type:application/json http://localhost:7474/streaming/cypher

A sample Parser/Client implementation is in org.neo4j.server.extension.streaming.cypher.CypherResultReader

The format is for a query like:

    start n=node(*) match p=n-[r]-m return  n as first,r as rel,m as second,m.name? as name,r.foo? as foo,ID(n) as id, p as path , NODES(p) as all

	// columns
	{"columns":["first","rel","second","name","foo","id","path","all"],
    // rows is an array of array of objects each object is { type : value }
	"rows":[
	    [{"Node":{"id":0,"data":{"name":42}}},
         {"Relationship":{"id":0,"start":0,"end":1,"type":"knows","data":{"name":"rel1"}}},
         {"Node":{"id":1,"data":{"name":"n2"}}},
         {"String":"n2"},
         {"Null":null},
         {"Long":0},
         {"Path":
             {"length":1,
              "start":{"id":0,"data":{"name":42}},
              "end":{"id":1,"data":{"name":"n2"}},
              "last_rel":{"id":0,"start":0,"end":1,"type":"knows","data":{"name":"rel1"}},
              "nodes":[{"id":0,"data":{"name":42}},{"id":1,"data":{"name":"n2"}}],
              "relationships":[{"id":0,"start":0,"end":1,"type":"knows","data":{"name":"rel1"}}]}},
         {"Array":[{"id":0,"data":{"name":42}},{"id":1,"data":{"name":"n2"}}]}]],
    // number of rows
	"count":1,
    // full runtime including streaming all the results
	"time":29}


header params/websocket format (in protocol field)

mode=none
mode=compact
mode=compat

format=pretty

## websocket protocol


* array of commands
* each command is: [opcode, selector, data]

* opcodes: ADD|DELETE|UPDATE _NODES | ADD|DELETE|UPDATE _RELS | CYPHER

* selector: id, [ids], ref, { index:key : value}, { index : query}, *
  (selector after opcode or as start, end for relationships, traversals etc.)

* ref is a lookup mechanism during command execution (in a context)

* data is a map or a list of maps depending on command

* streaming results

    function init() { ws = new WebSocket("ws://localhost:8080/command");ws.onmessage = function(evt) { console.log(evt.data);}}
    ws.send(JSON.stringify([["ADD_NODES",null,{data:{name:"foo"},ref:"foo", unique: { index: "test", key: "name", value:"foo"}}],["GET_NODES","*",null]]))

### commands

* ADD_NODES : data is single or array of { data : {props}, ref : "ref", index : {index: key: value:} | [{}], unique: {index: key: value:}}

* ADD_RELS : data is single or array of { data : {props}, ref : "ref",type:"type", start: selector, end : selector, index : {index: key: value: } | [{}], unique: {index: key: value:}}

* DELETE_NODES : uses selector , if data is { force : true } it also deletes the relationships
* DELETE_RELS : uses selector

* UPDATE_NODES, UPDATE_RELS : uses selector, data is array or single of { data : {props}, ref : "ref", index : {index: [key:] [value:] [old:]}}, null values delete properties and index entries, old index value will be removed
* CYPHER : data is { query : "query" , [params : { params}], useContext: true, mergeResult : true } useContext -> merges current context with params, merges cypher result with context