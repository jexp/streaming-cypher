package org.neo4j.server.extension.streaming.cypher;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author mh
 * @since 13.04.12
 */
class CypherService {
    private final ExecutionEngine engine;
    private final JsonFactory jsonFactory = new JsonFactory();

    CypherService(final GraphDatabaseService gdb) {
        engine = new ExecutionEngine(gdb);
    }


    void execute(String query, Map<String, Object> params, OutputStream output) throws IOException {
        long start = System.currentTimeMillis();
        final ExecutionResult result = engine.execute(query,params!=null ? params : Collections.<String,Object>emptyMap());
        toJson(result,start, output);
    }

    public void toJson(ExecutionResult result, long start, OutputStream output) throws IOException {
        JsonGenerator g = jsonFactory.createJsonGenerator(output);
        g.writeStartObject();
        final List<String> columns = result.columns();
        writeColumns(g, columns);
        final int count = writeRows(result, g, columns);
        g.writeNumberField("count", count);
        g.writeNumberField("time", System.currentTimeMillis() - start);
        g.writeEndObject();
        g.close();
    }

    private int writeRows(ExecutionResult result, JsonGenerator g, List<String> columns) throws IOException {
        g.writeArrayFieldStart("rows");
        int count=0;
        for (Map<String, Object> row : result) {
            count++;
            g.writeStartArray();
            for (String column : columns) {
                final Object value = row.get(column);
                g.writeStartObject();
                g.writeFieldName(type(value));
                writeValue(g, value);
                g.writeEndObject();
            }
            g.writeEndArray();
        }
        g.writeEndArray();
        return count;
    }

    private void writeColumns(JsonGenerator g, List<String> columns) throws IOException {
        g.writeArrayFieldStart("columns");
        for (String column : columns) {
            g.writeString(column);
        }
        g.writeEndArray();
    }

    private String type(final Object value) {
        if (value==null) return "Null";
        if (value instanceof Path) return "Path";
        if (value instanceof Node) return "Node";
        if (value instanceof Relationship) return "Relationship";
        if (value instanceof Iterable) return "Array";
        return value.getClass().getSimpleName();
    }
    /*
    
    [1,2,3]
    [{value:1,type:int},{value:"blub",type:"String"}]
     */
    
    private void writeValue(JsonGenerator g, Object value) throws IOException {
        if (value instanceof Node) {
            writeNode(g, (Node) value);
            return;
        }
        if (value instanceof Relationship) {
            writeRelationship(g, (Relationship) value);
            return;
        }
        if (value instanceof Path) {
            writePath(g, (Path) value);
            return;
        }
        if (value instanceof Iterable) {
            writeIterable(g, (Iterable) value);
            return;
        }
        g.writeObject(value);
    }

    private void writeIterable(JsonGenerator g, Iterable values) throws IOException {
        g.writeStartArray();
        for (Object value : values) {
            writeValue(g, value);
        }
        g.writeEndArray();
    }

    private void writeRelationship(JsonGenerator g, Relationship relationship) throws IOException {
        g.writeStartObject();
        g.writeNumberField("id", relationship.getId());
        g.writeNumberField("start", relationship.getStartNode().getId());
        g.writeNumberField("end", relationship.getEndNode().getId());
        g.writeStringField("type", relationship.getType().name());
        writePropertyContainer(g, relationship);
        g.writeEndObject();
    }

    private void writeNode(JsonGenerator g, Node node) throws IOException {
        g.writeStartObject();
        g.writeNumberField("id", node.getId());
        writePropertyContainer(g, node);
        g.writeEndObject();
    }

    private void writePath(JsonGenerator g, Path path) throws IOException {
        g.writeStartObject();
		g.writeNumberField("length", path.length());
        g.writeFieldName("start"); writeNode(g, path.startNode());
        g.writeFieldName("end"); writeNode(g, path.endNode());
        g.writeFieldName("last_rel"); writeRelationship(g, path.lastRelationship());
		g.writeArrayFieldStart("nodes");
        for (Node node : path.nodes()) {
            writeNode(g,node);
        }
		g.writeEndArray();
		g.writeArrayFieldStart("relationships");
        for (Relationship relationship : path.relationships()) {
            writeRelationship(g, relationship);
        }
		g.writeEndArray();
        g.writeEndObject();
    }

    private void writePropertyContainer(JsonGenerator g, PropertyContainer node) throws IOException {
        final Iterator<String> propertyKeys = node.getPropertyKeys().iterator();
        if (!propertyKeys.hasNext()) return;
        g.writeFieldName("props");
        g.writeStartObject();
        while (propertyKeys.hasNext()) {
            String prop = propertyKeys.next();
            g.writeObjectField(prop, node.getProperty(prop));
        }
        g.writeEndObject();
    }
}
