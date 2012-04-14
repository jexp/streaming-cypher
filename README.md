Just put the [jar](https://github.com/downloads/neo4j-contrib/streaming-cypher/streaming-cypher-extension-1.7.M03.jar) (streaming-cypher-extension-1.7.M03.jar) into neo4j-server/plugins and add this to the conf/neo4j-server.properties file

    org.neo4j.server.thirdparty_jaxrs_classes=org.neo4j.server.extension.streaming.cypher=/streaming

Then you can issue

    curl -d'{"query":"start n=node(*) return n"}' -H accept:application/json -H content-type:application/json http://localhost:7474/streaming/cypher

to query the graph and stream the results.

Note, for rendering the existing format of the Neo4j-REST API (without the additional discoverable URLs) use:

    curl -d'{"query":"start n=node(*) return n"}' -H accept:application/json;compat=true -H content-type:application/json http://localhost:7474/streaming/cypher

A sample Parser/Client implementation is in org.neo4j.server.extension.streaming.cypher.CypherResultReader

The format is for a query like:

    start n=node(*) match p=n-[r]-m return  n as first,r as rel,m as second,m.name? as name,r.foo? as foo,ID(n) as id, p as path , NODES(p) as all

	// columns
	{"columns":["first","rel","second","name","foo","id","path","all"],
    // rows is an array of array of objects each object is { type : value }
	"rows":[
	    [{"Node":{"id":0,"props":{"name":42}}},
         {"Relationship":{"id":0,"start":0,"end":1,"type":"knows","props":{"name":"rel1"}}},
         {"Node":{"id":1,"props":{"name":"n2"}}},
         {"String":"n2"},
         {"Null":null},
         {"Long":0},
         {"Path":
             {"length":1,
              "start":{"id":0,"props":{"name":42}},
              "end":{"id":1,"props":{"name":"n2"}},
              "last_rel":{"id":0,"start":0,"end":1,"type":"knows","props":{"name":"rel1"}},
              "nodes":[{"id":0,"props":{"name":42}},{"id":1,"props":{"name":"n2"}}],
              "relationships":[{"id":0,"start":0,"end":1,"type":"knows","props":{"name":"rel1"}}]}},
         {"Array":[{"id":0,"props":{"name":42}},{"id":1,"props":{"name":"n2"}}]}]],
    // number of rows
	"count":1,
    // full runtime including streaming all the results
	"time":29}
