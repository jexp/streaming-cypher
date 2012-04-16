package org.neo4j.server.extension.streaming.websocket;


import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketFactory;
import org.eclipse.jetty.websocket.WebSocketFactory.Acceptor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.server.extension.streaming.commands.CommandHandler;
import org.neo4j.server.extension.streaming.cypher.json.JsonResultWriter;
import org.neo4j.server.extension.streaming.cypher.json.JsonResultWriters;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

public class WebSocketServlet extends HttpServlet {

	private static WebSocketFactory factory                   = null;
    private GraphDatabaseService gdb;

    public WebSocketServlet(GraphDatabaseService gdb) {
        this.gdb = gdb;
    }

    public WebSocketServlet() {
    }

    // todo chunked
    static class WriterSelector {
        private boolean pretty;
        private boolean compat;
        private final JsonResultWriters writers;
        private final String requestUri;
        private boolean none;

        WriterSelector(HttpServletRequest request, String protocol) {
            final String accept = protocol!=null ? protocol : request.getHeader("Accept");
            pretty =    accept.contains("pretty");
            compat = accept.contains("compat");
            none = accept.contains("none");
            writers = new JsonResultWriters();
            requestUri = "http://localhost:7474/command"; // TODO request.getRequestURL().toString(); -> no uri exception, there is no uri in the request
        }

        public JsonResultWriter writeTo(OutputStream stream) throws IOException {
            final JsonResultWriter writer = compat ? writers.writeCompatTo(stream,restBaseUri(requestUri)) : none ? writers.writeNothingTo(stream) : writers.writeTo(stream);
            if (pretty) writer.usePrettyPrinter();
            return writer;
        }

        private String restBaseUri(String requestUri) {
            try {
                final URL url = new URL(requestUri);
                return String.format("%s://%s:%d/db/data/",url.getProtocol(),url.getHost(),url.getPort());
            } catch (MalformedURLException e) {
                throw new RuntimeException("Error resolving base REST URI from "+requestUri,e);
            }
        }
    }

    @Override
	public void init() {
        if (gdb==null) gdb = createDatabase();
        final CommandHandler commandHandler = new CommandHandler(gdb);
        final ObjectMapper objectMapper = new ObjectMapper();
        factory = new WebSocketFactory(new Acceptor() {
			public WebSocket doWebSocketConnect(final HttpServletRequest request, final String protocol) {
                // todo check protocol neo4j
		        return new WebSocket.OnTextMessage() {
                    private Connection connection;

                    public void onMessage(String msg) {
                        try {
                            final List<List> commands = objectMapper.readValue(msg, List.class);
                            commandHandler.handle(commands,new WebsocketJsonWriter(connection, new WriterSelector(request,protocol)));
                        } catch (Exception e) {
                            e.printStackTrace();
                            sendMessage("Exception: " + e.getMessage());
                            connection.close();
                        }
                    }

                    private void sendMessage(final String message) {
                        try {
                            connection.sendMessage(message);
                        } catch (IOException ioe) {
                            ioe.printStackTrace();
                            connection.close();
                        }
                    }

                    public void onOpen(Connection connection) {
                        this.connection = connection;
                    }

                    public void onClose(int i, String msg) {

                    }
                };
			}

            @Override
            public boolean checkOrigin(HttpServletRequest httpServletRequest, String origin) {
                return true;
            }
        });
	}

    private EmbeddedGraphDatabase createDatabase() {
        String database = getServletConfig().getInitParameter("database");
        if (database==null) database = "target/db";
        return new EmbeddedGraphDatabase(database);
    }

    @Override
	public void destroy() {
        try {
            gdb.shutdown();
            factory.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

	@Override
	protected void doGet(final HttpServletRequest request, HttpServletResponse response) throws IOException {
		if (!factory.acceptWebSocket(request, response)) {
			response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
		}
	}
}
