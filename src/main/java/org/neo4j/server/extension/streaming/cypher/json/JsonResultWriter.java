package org.neo4j.server.extension.streaming.cypher.json;

import org.neo4j.cypher.javacompat.ExecutionResult;

import java.io.IOException;

/**
* @author mh
* @since 14.04.12
*/
public interface JsonResultWriter {
    void toJson(ExecutionResult result, long start) throws IOException;

    JsonResultWriter usePrettyPrinter();
}
