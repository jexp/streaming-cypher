package org.neo4j.server.extension.streaming.cypher.json;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.ImpermanentGraphDatabase;

import java.io.*;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 13.04.12
 */
public class JsonFormatTest {

    private final ImpermanentGraphDatabase gdb = new ImpermanentGraphDatabase();

    @Before
    public void setUp() throws Exception {
        gdb.beginTx();
        gdb.getReferenceNode().setProperty("name","refNode");
    }

    @After
    public void tearDown() {
        gdb.shutdown();
    }

    @Test
    public void testCompactFormat() throws IOException, InterruptedException {
        final Map result = query(null);
        final List<String> columns = asList("node");
        assertEquals(columns,result.get("columns"));
        assertEquals(1,result.get("count"));
        List<List<Map<String,Object>>> rows= (List<List<Map<String, Object>>>) result.get("rows");
        final List<Map<String, Object>> row = rows.get(0);
        Map<String,Object> cell = row.get(0);
        final Map node = (Map) cell.get("Node");
        assertEquals((int) gdb.getReferenceNode().getId(), node.get("id"));
        assertEquals("refNode", ((Map)node.get("data")).get("name"));
    }

    @Test
    public void testCompatibleFormat() throws IOException, InterruptedException {
        final String uri = "http://localhost:7470/db/data";
        final Map result = query(uri);
        final List<String> columns = asList("node");
        assertEquals(columns,result.get("columns"));
        assertEquals(false,result.containsKey("count"));
        assertEquals(false,result.containsKey("time"));
        List<List<Map<String,Object>>> rows= (List<List<Map<String, Object>>>) result.get("data");
        final List<Map<String, Object>> row = rows.get(0);
        Map<String,Object> cell = row.get(0);
        final String nodeUri = String.format("%s/node/%d", uri, gdb.getReferenceNode().getId());
        assertEquals(nodeUri, cell.get("self"));
        assertEquals("refNode", ((Map) cell.get("data")).get("name"));
    }

    private Map query(String uri) throws IOException {
        ExecutionResult data = new ExecutionResultStub(asList("node"), MapUtil.map("node", gdb.getReferenceNode()), 1);
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final JsonResultWriters writers = new JsonResultWriters();
        final JsonResultWriter writer = uri==null ? writers.writeTo(stream) : writers.writeCompatTo(stream, uri);
        writer.writeResult(data, 0L);
        writer.close();
        return new ObjectMapper().readValue(stream.toString(), Map.class);
    }
}
