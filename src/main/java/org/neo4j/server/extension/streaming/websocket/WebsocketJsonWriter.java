package org.neo4j.server.extension.streaming.websocket;

import org.eclipse.jetty.websocket.WebSocket;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.server.extension.streaming.cypher.json.JsonResultWriter;
import org.neo4j.server.extension.streaming.cypher.json.JsonResultWriters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author mh
 * @since 16.04.12
 */
public class WebsocketJsonWriter implements JsonResultWriter {

    private final ByteArrayOutputStream stream;
    private final JsonResultWriter writer;
    private final WebSocket.Connection connection;

    public WebsocketJsonWriter(WebSocket.Connection connection, WebSocketServlet.WriterSelector writerSelector) throws IOException {
        this.connection = connection;
        stream = new ByteArrayOutputStream();
        writer = writerSelector.writeTo(stream);
    }

    @Override
    public void writeResult(ExecutionResult result, long start) throws IOException {
        writer.writeResult(result,start);
    }

    @Override
    public JsonResultWriter usePrettyPrinter() {
        return writer.usePrettyPrinter();
    }

    @Override
    public void startArray() throws IOException {
        writer.startArray();
    }

    @Override
    public void endArray() throws IOException {
        writer.endArray();
        // todo control chunked writing with config/headers sendStream();
    }

    @Override
    public void writeNode(Node node) throws IOException {
        writer.writeNode(node);
    }

    @Override
    public void writeRelationship(Relationship relationship) throws IOException {
        writer.writeRelationship(relationship);
    }

    @Override
    public void writePath(Path path) throws IOException {
        writer.writePath(path);
    }

    @Override
    public void close() throws IOException {
        writer.close();
        sendStream();
    }

    private void sendStream() throws IOException {
        if (stream.size()<=0) return;
        connection.sendMessage(stream.toString());
        stream.reset();
    }

}
