package org.neo4j.server.extension.streaming.cypher;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.extension.streaming.cypher.json.JsonResultWriter;
import org.neo4j.server.extension.streaming.cypher.json.JsonResultWriters;
import org.neo4j.test.ImpermanentGraphDatabase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 13.04.12
 */
public class CypherServiceTest {

    private static final int MILLION = 1000000;
    private final ImpermanentGraphDatabase gdb = new ImpermanentGraphDatabase();
    private Transaction tx;

    @Before
    public void setUp() {
        tx = gdb.beginTx();
    }

    @After
    public void tearDown() {
        tx.failure();
        tx.finish();
        gdb.shutdown();
    }

    @Test
    public void testCompactFormat() throws IOException {
        final Node refNode = gdb.getReferenceNode();
        refNode.setProperty("name", 42);
        final Node n2 = gdb.createNode();
        n2.setProperty("name", "n2");
        final Relationship rel = refNode.createRelationshipTo(n2, DynamicRelationshipType.withName("knows"));
        rel.setProperty("name", "rel1");
        final CypherService service = new CypherService(gdb);
        final String baseQuery = "start n=node(*) match p=n-[r]-m return ";
        final String simpleQuery = baseQuery + " NODES(p) as path";
        query(service, simpleQuery, null);
        query(service, simpleQuery, null);
        final String fullyQuery = baseQuery + " n as first,r as rel,m as second,m.name? as name,r.foo? as foo,ID(n) as id, p as path , NODES(p) as all";
        query(service, fullyQuery, null);
        final Map result = (Map) query(service, fullyQuery, null);
        final List<String> columns = asList("first", "rel", "second", "name", "foo", "id", "path", "all");
        assertEquals(columns,result.get("columns"));
        assertEquals(2,result.get("count"));
        List<List<Map<String,Object>>> rows= (List<List<Map<String, Object>>>) result.get("rows");
        final List<Map<String, Object>> row = rows.get(0);
        assertEquals((int)refNode.getId(), extract(columns, row, "first", Map.class).get("id"));
        assertEquals((int)n2.getId(), extract(columns, row, "second", Map.class).get("id"));
        assertEquals((int)rel.getId(), extract(columns, row, "rel",Map.class).get("id"));
        assertEquals(0, extract(columns, row, "id", Integer.class).intValue());
        assertEquals(null, extract(columns, row, "foo", Object.class));
        assertEquals("n2", extract(columns, row, "name",String.class));
        assertEquals(1, extract(columns, row, "path",Map.class).get("length"));
        assertEquals((int) refNode.getId(), ((Map) extract(columns, row, "all", List.class).get(0)).get("id"));
    }

    @Test
    public void testCompatibleFormat() throws IOException {
        final String uri = "http://localhost:7470/db/data/";
        final Node refNode = gdb.getReferenceNode();
        refNode.setProperty("name", 42);
        final Node n2 = gdb.createNode();
        n2.setProperty("name", "n2");
        final Relationship rel = refNode.createRelationshipTo(n2, DynamicRelationshipType.withName("knows"));
        rel.setProperty("name", "rel1");
        final CypherService service = new CypherService(gdb);
        final String baseQuery = "start n=node(*) match p=n-[r]-m return ";
        final String simpleQuery = baseQuery + " NODES(p) as path";
        query(service, simpleQuery, uri);
        query(service, simpleQuery, uri);
        final String fullyQuery = baseQuery + " n as first,r as rel,m as second,m.name? as name,r.foo? as foo,ID(n) as id, p as path , NODES(p) as all";
        query(service, fullyQuery, uri);
        final Map result = (Map) query(service, fullyQuery, uri);
        final List<String> columns = asList("first", "rel", "second", "name", "foo", "id", "path", "all");
        assertEquals(columns,result.get("columns"));
        assertEquals(false,result.containsKey("count"));
        List<List<Object>> rows= (List<List<Object>>) result.get("data");
        final List<Object> row = rows.get(0);
        final String refNodeUri = uri + "node/" + refNode.getId();
        final String node2NodeUri = uri + "node/" + n2.getId();
        final String relNodeUri = uri + "relationship/" + rel.getId();
        assertEquals(refNodeUri, extract2(columns, row, "first", Map.class).get("self"));
        assertEquals(42, ((Map)extract2(columns, row, "first", Map.class).get("data")).get("name"));
        assertEquals(node2NodeUri, extract2(columns, row, "second", Map.class).get("self"));
        assertEquals(relNodeUri, extract2(columns, row, "rel", Map.class).get("self"));
        assertEquals(0, extract2(columns, row, "id", Integer.class).intValue());
        assertEquals(null, extract2(columns, row, "foo", Object.class));
        assertEquals("n2", extract2(columns, row, "name", String.class));
        assertEquals(1, extract2(columns, row, "path", Map.class).get("length"));
        assertEquals(refNodeUri, extract2(columns, row, "path", Map.class).get("start"));
        assertEquals(node2NodeUri, extract2(columns, row, "path", Map.class).get("end"));
        assertEquals(refNodeUri, ((Map) extract2(columns, row, "all", List.class).get(0)).get("self"));
    }

    private <T> T extract(List<String> columns, List<Map<String, Object>> row, final String column, Class<T> type) {
        final int columnIndex = columns.indexOf(column);
        final Map<String, Object> cell = row.get(columnIndex);
        return (T)cell.values().iterator().next();
    }
    private <T> T extract2(List<String> columns, List<Object> row, final String column, Class<T> type) {
        final int columnIndex = columns.indexOf(column);
        return (T) row.get(columnIndex);
    }

    private Object query(CypherService service, final String query, String uri) throws IOException {
        System.out.println(query);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final JsonResultWriter writer = uri == null ? new JsonResultWriters().writeTo(baos) : new JsonResultWriters().writeCompatTo(baos, uri);
        service.execute(query, null, writer); // n as first,r as rel,m as second,m.name? as name,ID(n) as id, , NODES(p) as nodes
        System.out.println(baos.toString("UTF-8").replaceAll("([}\\]],)", "$1\n"));
        return new ObjectMapper().readValue(baos.toString(), Object.class);
    }
}
