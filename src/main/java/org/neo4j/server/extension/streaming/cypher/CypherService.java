package org.neo4j.server.extension.streaming.cypher;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.server.extension.streaming.cypher.json.JsonResultWriter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * @author mh
 * @since 13.04.12
 */
class CypherService {
    private final ExecutionEngine engine;

    CypherService(final GraphDatabaseService gdb) {
        engine = new ExecutionEngine(gdb);
    }

    void execute(String query, Map<String, Object> params, JsonResultWriter writer) throws IOException {
        long start = System.currentTimeMillis();
        final ExecutionResult result = engine.execute(query,params!=null ? params : Collections.<String,Object>emptyMap());
        writer.toJson(result, start);
    }
}
