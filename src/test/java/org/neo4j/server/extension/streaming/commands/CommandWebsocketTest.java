package org.neo4j.server.extension.streaming.commands;

import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketClient;
import org.eclipse.jetty.websocket.WebSocketClientFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.extension.streaming.websocket.WebSocketServlet;
import org.neo4j.test.ImpermanentGraphDatabase;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 02.03.11
 */
public class CommandWebsocketTest {

    private static Server server;
    private static ImpermanentGraphDatabase gdb;

    @BeforeClass
    public static void startServer() throws Exception {
        server = new Server(8080);
        gdb = new ImpermanentGraphDatabase();
        ServletHandler servletHandler = new ServletHandler();
        servletHandler.addServletWithMapping(new ServletHolder(new WebSocketServlet(gdb)),"/command/*");
        //servletHandler.addServletWithMapping(new ServletHolder(WebSocketServlet.class),"/command/*");
        server.setHandler(servletHandler);
        server.start();
    }

    @AfterClass
    public static void shutdownServer() throws Exception {
        server.stop();
    }

    @Test
    public void executeCommand() throws Exception {
        System.out.println("server.isRunning() = " + server.isRunning());
        final String uri = createCommandURI();
        System.out.println("uri = " + uri);
        final Map<String, Object> props = map("name", "foo");
        final Collection<List> commands = Collections.<List>singletonList(asList("ADD_NODES", null, map("data", props)));

        final CountDownLatch latch = new CountDownLatch(1);
        WebSocketClientFactory factory = new WebSocketClientFactory();
        factory.setBufferSize(4096);
        factory.start();
        WebSocketClient client = factory.newWebSocketClient();
        client.setMaxIdleTime(30000);
        client.setMaxTextMessageSize(1024*1024);
        client.setProtocol("xx");
        Future<WebSocket.Connection> future = client.open(new URI(createCommandURI()),new WebSocket.OnTextMessage(){
            @Override
            public void onOpen(Connection connection) { }

            @Override
            public void onClose(int i, String s) { }

            @Override
            public void onMessage(String data) {
                System.out.println("data = " + data);
                assertEquals("foo", gdb.getNodeById(1).getProperty("name"));
                latch.countDown();
            }
        });

        final WebSocket.Connection connection = future.get();
        long time = System.currentTimeMillis();
        final String data = new ObjectMapper().writeValueAsString(commands);
        connection.sendMessage(data);
        latch.await();
        System.out.println("Took "+(System.currentTimeMillis()-time)+" ms.");
    }

    private String createCommandURI() throws URISyntaxException, MalformedURLException {
        return "ws://localhost:8080/command";
    }
}
