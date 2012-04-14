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
    }

    class JsonWriter implements JsonResultWriter {
        protected OutputStream output;
        private boolean pretty;

        JsonWriter(OutputStream output) {
            this.output = output;
        }

        @Override
        public JsonResultWriter usePrettyPrinter() {
            this.pretty=true;
            return this;
        }

        public void toJson(ExecutionResult result, long start) throws IOException {
            JsonGenerator g = createGenerator();
            g.writeStartObject();
            final List<String> columns = result.columns();
            writeColumns(g, columns);
            final int count = writeRows(result, g, columns);
            writeCount(g, count);
            writeTime(g, start);
            g.writeEndObject();
            g.close();
        }

        protected JsonGenerator createGenerator() throws IOException {
            JsonGenerator g = jsonFactory.createJsonGenerator(output);
            if (pretty) {
                g.useDefaultPrettyPrinter();
            }
            return g;
        }

        protected void writeTime(JsonGenerator g, long start) throws IOException {
            g.writeNumberField("time", System.currentTimeMillis() - start);
        }

        protected void writeCount(JsonGenerator g, int count) throws IOException {
            g.writeNumberField("count", count);
        }

        protected int writeRows(ExecutionResult result, JsonGenerator g, List<String> columns) throws IOException {
            g.writeArrayFieldStart("rows");
            int count = 0;
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

        protected void writeColumns(JsonGenerator g, List<String> columns) throws IOException {
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

        protected void writeValue(JsonGenerator g, Object value) throws IOException {
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

        protected void writeIterable(JsonGenerator g, Iterable values) throws IOException {
            g.writeStartArray();
            for (Object value : values) {
                writeValue(g, value);
            }
            g.writeEndArray();
        }

        protected void writeRelationship(JsonGenerator g, Relationship relationship) throws IOException {
            g.writeStartObject();
            writeId(g, relationship);
            writeRef(g, "start", relationship.getStartNode());
            writeRef(g, "end", relationship.getEndNode());
            g.writeStringField("type", relationship.getType().name());
            writePropertyContainer(g, relationship);
            g.writeEndObject();
        }

        protected void writeId(JsonGenerator g, Relationship relationship) throws IOException {
            writeRef(g, "id", relationship);
        }

        protected void writeNode(JsonGenerator g, Node node) throws IOException {
            g.writeStartObject();
            writeId(g, node);
            writePropertyContainer(g, node);
            g.writeEndObject();
        }

        protected void writeId(JsonGenerator g, Node node) throws IOException {
            writeRef(g, "id", node);
        }

        protected void writePath(JsonGenerator g, Path path) throws IOException {
            g.writeStartObject();
            g.writeNumberField("length", path.length());
            g.writeFieldName("start");
            writeNode(g, path.startNode());
            g.writeFieldName("end");
            writeNode(g, path.endNode());
            g.writeFieldName("last_rel");
            writeRelationship(g, path.lastRelationship());
            g.writeArrayFieldStart("nodes");
            for (Node node : path.nodes()) {
                writeNode(g, node);
            }
            g.writeEndArray();
            g.writeArrayFieldStart("relationships");
            for (Relationship relationship : path.relationships()) {
                writeRelationship(g, relationship);
            }
            g.writeEndArray();
            g.writeEndObject();
        }

        protected void writePropertyContainer(JsonGenerator g, PropertyContainer node) throws IOException {
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

        protected  void writeRef(JsonGenerator g, String fieldName, Node node) throws IOException {
            g.writeNumberField(fieldName, node.getId());
        }

        protected  void writeRef(JsonGenerator g, String fieldName, Relationship relationship) throws IOException {
            g.writeNumberField(fieldName, relationship.getId());
        }
    }

    class JsonCompatWriter extends JsonWriter {
        protected final String uri;

        JsonCompatWriter(OutputStream output, String uri) {
            super(output);
            this.uri = uri.endsWith("/") ? uri : uri + "/";
        }

        protected int writeRows(ExecutionResult result, JsonGenerator g, List<String> columns) throws IOException {
            g.writeArrayFieldStart("data");
            int count = 0;
            for (Map<String, Object> row : result) {
                count++;
                g.writeStartArray();
                for (String column : columns) {
                    writeValue(g, row.get(column));
                }
                g.writeEndArray();
            }
            g.writeEndArray();
            return count;
        }

        @Override
        protected void writeTime(JsonGenerator g, long start) throws IOException {
        }

        @Override
        protected void writeCount(JsonGenerator g, int count) throws IOException {
        }

        @Override
        protected void writeId(JsonGenerator g, Relationship relationship) throws IOException {
            writeRef(g,"self",relationship);
        }

        @Override
        protected void writeId(JsonGenerator g, Node node) throws IOException {
            writeRef(g,"self",node);
        }

        @Override
        protected void writeRef(JsonGenerator g, String fieldName, Node node) throws IOException {
            g.writeStringField(fieldName, nodeUri(node));
        }

        private String nodeUri(Node node) {
            return uri + "node/" + node.getId();
        }

        @Override
        protected void writeRef(JsonGenerator g, String fieldName, Relationship relationship) throws IOException {
            g.writeStringField(fieldName, relationshipUri(relationship));
        }

        private String relationshipUri(Relationship relationship) {
            return uri + "relationship/" + relationship.getId();
        }

        protected void writePath(JsonGenerator g, Path path) throws IOException {
            g.writeStartObject();
            g.writeNumberField("length", path.length());
            writeRef(g, "start", path.startNode());
            writeRef(g,"end",path.endNode());

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

    public JsonResultWriter writeCompatTo(OutputStream output, String uri) {
        return new JsonCompatWriter(output, uri);
    }

    public JsonResultWriter writeTo(OutputStream output) {
        return new JsonWriter(output);
    }
}
