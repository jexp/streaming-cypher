package org.neo4j.server.extension.streaming.cypher.json;

import org.codehaus.jackson.JsonGenerationException;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;

/**
* @author mh
* @since 14.04.12
*/
public interface JsonResultWriter {
    void writeResult(ExecutionResult result, long start) throws IOException;

    JsonResultWriter usePrettyPrinter();

    void startArray() throws IOException;

    void endArray() throws IOException;

    void writeNode(Node node) throws IOException;

    void writeRelationship(Relationship relationship) throws IOException;

    void writePath(Path path) throws IOException;

    void close() throws IOException;
}
