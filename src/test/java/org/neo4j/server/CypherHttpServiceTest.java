package org.neo4j.server;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.extension.LocalTestServer;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author mh
 * @since 02.03.11
 */
public class CypherHttpServiceTest {
    private static LocalTestServer neoServer = new LocalTestServer();

    private static final String CONTEXT_PATH = "streaming/cypher";
    private static final int FEW_NODES = 10;

    @BeforeClass
    public static void startServerWithACleanDb() {
        neoServer.start();
    }

    @AfterClass
    public static void shutdownServer() {
        neoServer.stop();
    }

    private GraphDatabaseService getGraphDb() {
        return neoServer.getDatabase().graph;
    }


    @Test
    public void queryAllNodes() throws Exception {
        createData(getGraphDb(), FEW_NODES);
        String query = "start n=node(*) match p=n-[r]-m return n,r,m,p";
        final String uri = createQueryURI();
        System.out.println("uri = " + uri);
        ClientResponse response = Client.create().resource(uri).post(ClientResponse.class,new ObjectMapper().writeValueAsString(MapUtil.map("query",query)));
        assertEquals(ClientResponse.Status.OK.getStatusCode(), response.getStatus());
        final String result = response.getEntity(String.class);
        System.out.println(result);
        response.close();
        final Map data = new ObjectMapper().readValue(result, Map.class);
        assertEquals(true, data.containsKey("count"));
        assertEquals(true, data.containsKey("time"));
        assertEquals(asList("n","r","m","p"), data.get("columns"));
        List<List<Map<String,Object>>> rows= (List<List<Map<String, Object>>>) data.get("rows");
        assertEquals(data.get("count"), rows.size());
        final List<Map<String, Object>> row = rows.get(0);
        assertTrue(((Map) row.get(1).get("Relationship")).get("type").toString().startsWith("TEST_"));
    }

    @Test
    public void queryAllNodesCompatible() throws Exception {
        createData(getGraphDb(), FEW_NODES);
        String query = "start n=node(*) match p=n-[r]-m return n,r,m,p";
        final String uri = createQueryURI();
        System.out.println("uri = " + uri);
        ClientResponse response = Client.create().resource(uri).header("Accept","application/json;mode=compat" +
                "").post(ClientResponse.class, new ObjectMapper().writeValueAsString(MapUtil.map("query", query)));
        assertEquals(ClientResponse.Status.OK.getStatusCode(), response.getStatus());
        final String result = response.getEntity(String.class);
        System.out.println(result);
        response.close();
        final Map data = new ObjectMapper().readValue(result, Map.class);
        assertEquals(false, data.containsKey("count"));
        assertEquals(false, data.containsKey("time"));
        assertEquals(asList("n","r","m","p"), data.get("columns"));
        List<List<Object>> rows= (List<List<Object>>) data.get("data");
        final List<Object> row = rows.get(0);
        assertTrue(((Map) row.get(1)).get("type").toString().startsWith("TEST_"));
    }


    @Test
    public void queryPrettyPrint() throws Exception {
        String query = "start n=node(0) return n";
        ClientResponse response = Client.create().resource(createQueryURI()).header("Accept","application/json;format=pretty")
                .post(ClientResponse.class, new ObjectMapper().writeValueAsString(MapUtil.map("query", query)));
        assertEquals(ClientResponse.Status.OK.getStatusCode(), response.getStatus());
        final String result = response.getEntity(String.class);
        System.out.println(result);
        assertTrue(result.contains("\n "));
        response.close();
    }
    private String createQueryURI() {
        return neoServer.baseUri().toString() + CONTEXT_PATH;
    }

    private void createData(GraphDatabaseService db, int max) {
        Transaction tx = db.beginTx();
        try {
            final IndexManager indexManager = db.index();
            Node[] nodes = new Node[max];
            for (int i = 0; i < max; i++) {
                nodes[i] = db.createNode();
                final Index<Node> index = indexManager.forNodes("node_index_" + String.valueOf(i % 5));
                index.add(nodes[i],"ID",i);
            }
            Random random = new Random();
            for (int i = 0; i < max * 2; i++) {
                int from = random.nextInt(max);
                final int to = (from + 1 + random.nextInt(max - 1)) % max;
                final Relationship relationship = nodes[from].createRelationshipTo(nodes[to], DynamicRelationshipType.withName("TEST_" + i));
                final Index<Relationship> index = indexManager.forRelationships("rel_index_" + String.valueOf(i % 5));
                index.add(relationship, "ID", i);
            }
            tx.success();
        } finally {
            tx.finish();
        }
    }
}
