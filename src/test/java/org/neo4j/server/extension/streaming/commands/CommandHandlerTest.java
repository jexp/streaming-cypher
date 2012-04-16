package org.neo4j.server.extension.streaming.commands;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Relationship;
import org.neo4j.server.extension.streaming.cypher.json.CountingOutputStream;
import org.neo4j.server.extension.streaming.cypher.json.JsonResultWriter;
import org.neo4j.server.extension.streaming.cypher.json.JsonResultWriters;
import org.neo4j.test.ImpermanentGraphDatabase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 16.04.12
 */
public class CommandHandlerTest {
    private static final int COUNT = 10000;
    private ImpermanentGraphDatabase gdb = new ImpermanentGraphDatabase();
    private CommandHandler handler;

    @Before
    public void setUp() throws Exception {
        handler = new CommandHandler(gdb);
    }

    @Test
    public void testCreateRelationship() throws IOException {
        final Map<String, Object> props = map("name", "foo");
        final Collection<List> commands = Arrays.<List>asList(
                asList("ADD_NODES", null, map("data", props, "ref", "foo")),
                asList("ADD_RELS",  null, map("data", map("name","rel1"), "start", 0, "end", "foo", "type", "KNOWS"))
        );
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        handler.handle(commands, new JsonResultWriters().writeTo(stream));
        System.out.println(stream);
        final Relationship rel = gdb.getRelationshipById(0);
        assertEquals(gdb.getReferenceNode(), rel.getStartNode());
        assertEquals(gdb.getNodeById(1), rel.getEndNode());
        assertEquals("KNOWS", rel.getType().name());
        assertEquals("rel1", rel.getProperty("name"));
    }
    @Test
    public void testAddNode() throws Exception {
        final Map<String, Object> props = map("name", "foo");
        final Collection<List> commands = Collections.<List>singletonList(asList("ADD_NODES", null, map("data", props)));
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        handler.handle(commands, new JsonResultWriters().writeTo(stream));
        final String result = stream.toString();
        System.out.println(result);
        final List<List<Map>> resultData = new ObjectMapper().readValue(result, List.class);
        assertEquals("foo", gdb.getNodeById(1).getProperty("name"));
        final Map nodeData = resultData.get(0).get(0);
        assertEquals("one command",1,resultData.size());
        assertEquals("one node",1,resultData.get(0).size());
        assertEquals("id", 1, nodeData.get("id"));
        assertEquals(props, nodeData.get("data"));
    }
    @Test
    public void testUpdateNode() throws Exception {
        final Map<String, Object> props = map("name", "foo");
        final Collection<List> commands = Collections.<List>singletonList(asList("Update_NODES", 0, map("data", props)));
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        handler.handle(commands, new JsonResultWriters().writeTo(stream));
        final String result = stream.toString();
        System.out.println(result);
        assertEquals("foo", gdb.getNodeById(0).getProperty("name"));
        // todo no updated nodes sent back
        /*
        final List<List<Map>> resultData = new ObjectMapper().readValue(result, List.class);
        final Map nodeData = resultData.get(0).get(0);
        assertEquals("one command",1,resultData.size());
        assertEquals("one node",1,resultData.get(0).size());
        assertEquals("id", 1, nodeData.get("id"));
        assertEquals(props, nodeData.get("data"));
        */
    }
    @Test @Ignore("slow test")
    public void testAddManyNodes() throws Exception {
        final Map<String, Object> data = map("data", map("name", "foo"));
        List<Map> nodes = new ArrayList<Map>(COUNT);
        for (int i=0;i<COUNT;i++) {
            nodes.add(data);
        }
        final Collection<List> commands = Collections.<List>singletonList(asList("ADD_NODES", null, nodes));
        final CountingOutputStream stream = new CountingOutputStream();
        long time=System.currentTimeMillis();
        final JsonResultWriter writer = new JsonResultWriters().writeTo(stream);
        handler.handle(commands, writer);
        System.out.println("Creating "+COUNT+" nodes took "+(System.currentTimeMillis()-time)+" ms for "+stream.getCount()+" bytes");
        assertEquals("foo", gdb.getNodeById(2).getProperty("name"));
        assertEquals("foo", gdb.getNodeById(COUNT).getProperty("name"));
    }
}
