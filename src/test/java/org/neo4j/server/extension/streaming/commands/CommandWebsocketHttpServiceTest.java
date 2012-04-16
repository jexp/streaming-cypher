package org.neo4j.server.extension.streaming.commands;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketClient;
import org.eclipse.jetty.websocket.WebSocketClientFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;
import org.neo4j.server.extension.LocalTestServer;
import org.neo4j.server.extension.streaming.websocket.WebsocketExtensionInitializer;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 02.03.11
 */
@Ignore("need upgrade of jetty in neo4j-server to 8.x")
public class CommandWebsocketHttpServiceTest {
    private static LocalTestServer neoServer = new LocalTestServer("localhost", 7470, new ThirdPartyJaxRsPackage(WebsocketExtensionInitializer.class.getPackage().getName(), "/websocket"));

    @BeforeClass
    public static void startServerWithACleanDb() {
        neoServer.start();
    }

    @AfterClass
    public static void shutdownServer() {
        neoServer.stop();
    }

    @Test
    public void executeCommand() throws Exception {
        final String uri = createCommandURI();
        System.out.println("uri = " + uri);
        final Map<String, Object> props = map("name", "foo");
        final Collection<List> commands = Collections.<List>singletonList(asList("ADD_NODES", null, map("data", props)));

        WebSocketClientFactory factory = new WebSocketClientFactory();
        factory.setBufferSize(4096);
        factory.start();
        WebSocketClient client = factory.newWebSocketClient();
        client.setMaxIdleTime(30000);
        client.setMaxTextMessageSize(1024);
        client.setProtocol("xx");
        Future<WebSocket.Connection> future = client.open(new URI(createCommandURI()),new WebSocket.OnTextMessage(){
            @Override
            public void onOpen(Connection connection) {
                System.out.println("connection = " + connection);
            }

            @Override
            public void onClose(int i, String s) {
            }


            @Override
            public void onMessage(String data) {
                System.out.println("data = " + data);
                assertEquals("foo", neoServer.getGraphDatabase().getNodeById(1).getProperty("name"));
            }
        });

        final WebSocket.Connection connection = future.get();
        System.out.println("connection = " + connection);
        final String data = new ObjectMapper().writeValueAsString(commands);
        connection.sendMessage(data);

    }

    private String createCommandURI() throws URISyntaxException, MalformedURLException {
        final URI uri = neoServer.baseUri();
        return new URI("ws",null,uri.getHost(),uri.getPort(),"/command",null,null).toString();
    }
}
