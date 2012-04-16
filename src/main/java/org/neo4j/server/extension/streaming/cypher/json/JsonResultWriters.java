package org.neo4j.server.extension.streaming.cypher.json;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author mh
 * @since 14.04.12
 */
public class JsonResultWriters {
    protected final JsonFactory jsonFactory;

    public JsonResultWriters() {
        this(new JsonFactory(new ObjectMapper()));
    }

    JsonResultWriters(final JsonFactory jsonFactory) {
        this.jsonFactory = jsonFactory;
        this.jsonFactory.enable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM);
    }

    private static class NullJsonResultWriter implements JsonResultWriter {
        public void writeResult(ExecutionResult result, long start) throws IOException { }

        public JsonResultWriter usePrettyPrinter() { return this; }

        public void startArray() throws IOException { }

        public void endArray() throws IOException { }

        public void writeNode(Node node) throws IOException { }

        public void writeRelationship(Relationship relationship) throws IOException { }

        public void writePath(Path path) throws IOException { }

        public void close() throws IOException { }
    }

    class JsonWriter implements JsonResultWriter {
        protected OutputStream output;
        protected final JsonGenerator g;

        JsonWriter(OutputStream output) throws IOException {
            this.output = output;
            g = jsonFactory.createJsonGenerator(this.output);
        }

        public void close() throws IOException {
            g.flush();
            g.close();
        }

        @Override
        public void startArray() throws IOException {
            g.writeStartArray();
        }

        @Override
        public void endArray() throws IOException {
            g.writeEndArray();
            g.flush();
        }

        @Override
        public JsonResultWriter usePrettyPrinter() {
            g.useDefaultPrettyPrinter();
            return this;
        }

        public void writeResult(ExecutionResult result, long start) throws IOException {
            g.writeStartObject();
            final List<String> columns = result.columns();
            writeColumns(columns);
            final int count = writeRows(result, columns);
            writeCount(count);
            writeTime(start);
            g.writeEndObject();
        }

        protected void writeTime(long start) throws IOException {
            g.writeNumberField("time", System.currentTimeMillis() - start);
        }

        protected void writeCount(int count) throws IOException {
            g.writeNumberField("count", count);
        }

        protected int writeRows(ExecutionResult result, List<String> columns) throws IOException {
            g.writeArrayFieldStart("rows");
            int count = 0;
            for (Map<String, Object> row : result) {
                count++;
                g.writeStartArray();
                for (String column : columns) {
                    final Object value = row.get(column);
                    g.writeStartObject();
                    g.writeFieldName(type(value));
                    writeValue(value);
                    g.writeEndObject();
                }
                g.writeEndArray();
            }
            g.writeEndArray();
            return count;
        }

        protected void writeColumns(List<String> columns) throws IOException {
            g.writeArrayFieldStart("columns");
            for (String column : columns) {
                g.writeString(column);
            }
            g.writeEndArray();
        }

        protected String type(final Object value) {
            if (value == null) return "Null";
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

        protected void writeValue(Object value) throws IOException {
            if (value instanceof Node) {
                writeNode((Node) value);
                return;
            }
            if (value instanceof Relationship) {
                writeRelationship((Relationship) value);
                return;
            }
            if (value instanceof Path) {
                writePath((Path) value);
                return;
            }
            if (value instanceof Iterable) {
                writeIterable((Iterable) value);
                return;
            }
            g.writeObject(value);
        }

        protected void writeIterable(Iterable values) throws IOException {
            g.writeStartArray();
            for (Object value : values) {
                writeValue(value);
            }
            g.writeEndArray();
        }

        public void writeRelationship(Relationship relationship) throws IOException {
            g.writeStartObject();
            writeId(relationship);
            writeRef("start", relationship.getStartNode());
            writeRef("end", relationship.getEndNode());
            g.writeStringField("type", relationship.getType().name());
            writePropertyContainer(relationship);
            g.writeEndObject();
        }

        protected void writeId(Relationship relationship) throws IOException {
            writeRef("id", relationship);
        }

        public void writeNode(Node node) throws IOException {
            g.writeStartObject();
            writeId(node);
            writePropertyContainer(node);
            g.writeEndObject();
        }

        protected void writeId(Node node) throws IOException {
            writeRef("id", node);
        }

        public void writePath(Path path) throws IOException {
            g.writeStartObject();
            g.writeNumberField("length", path.length());
            g.writeFieldName("start");
            writeNode(path.startNode());
            g.writeFieldName("end");
            writeNode(path.endNode());
            g.writeFieldName("last_rel");
            writeRelationship(path.lastRelationship());
            g.writeArrayFieldStart("nodes");
            for (Node node : path.nodes()) {
                writeNode(node);
            }
            g.writeEndArray();
            g.writeArrayFieldStart("relationships");
            for (Relationship relationship : path.relationships()) {
                writeRelationship(relationship);
            }
            g.writeEndArray();
            g.writeEndObject();
        }

        protected void writePropertyContainer(PropertyContainer node) throws IOException {
            final Iterator<String> propertyKeys = node.getPropertyKeys().iterator();
            if (!propertyKeys.hasNext()) return;
            g.writeFieldName("data");
            g.writeStartObject();
            while (propertyKeys.hasNext()) {
                String prop = propertyKeys.next();
                g.writeObjectField(prop, node.getProperty(prop));
            }
            g.writeEndObject();
        }

        protected  void writeRef(String fieldName, Node node) throws IOException {
            g.writeNumberField(fieldName, node.getId());
        }

        protected  void writeRef(String fieldName, Relationship relationship) throws IOException {
            g.writeNumberField(fieldName, relationship.getId());
        }
    }

    class JsonCompatWriter extends JsonWriter {
        protected final String uri;

        JsonCompatWriter(OutputStream output, String uri) throws IOException {
            super(output);
            this.uri = uri.endsWith("/") ? uri : uri + "/";
        }

        protected int writeRows(ExecutionResult result, List<String> columns) throws IOException {
            g.writeArrayFieldStart("data");
            int count = 0;
            for (Map<String, Object> row : result) {
                count++;
                g.writeStartArray();
                for (String column : columns) {
                    writeValue(row.get(column));
                }
                g.writeEndArray();
            }
            g.writeEndArray();
            return count;
        }

        @Override
        protected void writeTime(long start) throws IOException {
        }

        @Override
        protected void writeCount(int count) throws IOException {
        }

        @Override
        protected void writeId(Relationship relationship) throws IOException {
            writeRef("self",relationship);
        }

        @Override
        protected void writeId(Node node) throws IOException {
            writeRef("self",node);
        }

        @Override
        protected void writeRef(String fieldName, Node node) throws IOException {
            g.writeStringField(fieldName, nodeUri(node));
        }

        private String nodeUri(Node node) {
            return uri + "node/" + node.getId();
        }

        @Override
        protected void writeRef(String fieldName, Relationship relationship) throws IOException {
            g.writeStringField(fieldName, relationshipUri(relationship));
        }

        private String relationshipUri(Relationship relationship) {
            return uri + "relationship/" + relationship.getId();
        }

        public void writePath(Path path) throws IOException {
            g.writeStartObject();
            g.writeNumberField("length", path.length());
            writeRef("start", path.startNode());
            writeRef("end",path.endNode());

            g.writeArrayFieldStart("nodes");
            for (Node node : path.nodes()) {
                g.writeString(nodeUri(node));
            }
            g.writeEndArray();
            g.writeArrayFieldStart("relationships");
            for (Relationship relationship : path.relationships()) {
                g.writeString(relationshipUri(relationship));
            }
            g.writeEndArray();
            g.writeEndObject();
        }
    }

    public JsonResultWriter writeCompatTo(OutputStream output, String uri) throws IOException {
        return new JsonCompatWriter(output, uri);
    }

    public JsonResultWriter writeTo(OutputStream output) throws IOException {
        return new JsonWriter(output);
    }
    public JsonResultWriter writeNothingTo(OutputStream output) throws IOException {
        return new NullJsonResultWriter();
    }
}
