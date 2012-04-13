package org.neo4j.server.extension.streaming.cypher;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.ImpermanentGraphDatabase;

import java.io.*;
import java.util.Iterator;
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
    public void testFormat() throws IOException {
        final Node refNode = gdb.getReferenceNode();
        refNode.setProperty("name", 42);
        final Node n2 = gdb.createNode();
        n2.setProperty("name", "n2");
        final Relationship rel = refNode.createRelationshipTo(n2, DynamicRelationshipType.withName("knows"));
        rel.setProperty("name", "rel1");
        final CypherService service = new CypherService(gdb);
        final String baseQuery = "start n=node(*) match p=n-[r]-m return ";
        final String simpleQuery = baseQuery + " NODES(p) as path";
        query(service, simpleQuery);
        query(service, simpleQuery);
        final String fullyQuery = baseQuery + " n as first,r as rel,m as second,m.name? as name,r.foo? as foo,ID(n) as id, p as path , NODES(p) as all";
        query(service, fullyQuery);
        final Map result = (Map) query(service, fullyQuery);
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
        assertEquals((int)refNode.getId(), ((Map)extract(columns, row, "all",List.class).get(0)).get("id"));
    }

    private <T> T extract(List<String> columns, List<Map<String, Object>> row, final String column, Class<T> type) {
        final int columnIndex = columns.indexOf(column);
        final Map<String, Object> cell = row.get(columnIndex);
        return (T)cell.values().iterator().next();
    }

    private Object query(CypherService service, final String query) throws IOException {
        System.out.println(query);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        service.execute(query, null, baos); // n as first,r as rel,m as second,m.name? as name,ID(n) as id, , NODES(p) as nodes
        System.out.println(baos.toString("UTF-8").replaceAll("([}\\]],)", "$1\n"));
        return new ObjectMapper().readValue(baos.toString(), Object.class);
    }


    @Test
    public void testReadStreamingOneMillionNodes() throws IOException, InterruptedException {
        final Node refNode = gdb.getReferenceNode();
        refNode.setProperty("name", "Peter");
        refNode.setProperty("age", 39);

        final PipedInputStream inputStream = new PipedInputStream(10 * 1024 * 1024);
        final Thread thread = new Thread() {
            public void run() {
                try {
                    ExecutionResult result = new ExecutionResultStub(asList("node"), MapUtil.map("node", gdb.getReferenceNode()), MILLION);
                    final PipedOutputStream stream = new PipedOutputStream(inputStream);
                    new CypherService(gdb).toJson(result, System.currentTimeMillis(), stream);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            }
        };
        thread.start();
        final long start = System.currentTimeMillis();
        new CypherResultReader().readCypherResults(inputStream, new CypherResultReader.ResultCallback());
        System.out.println("Reading streaming results took " + (System.currentTimeMillis() - start) + " ms.");
        thread.join();
    }

    @Test
    public void testStreamOneMillionNodes() throws IOException {
        final Node refNode = gdb.getReferenceNode();
        refNode.setProperty("name", "Peter");
        refNode.setProperty("age", 39);
        ExecutionResult result = new ExecutionResultStub(asList("node"), MapUtil.map("node", gdb.getReferenceNode()), MILLION);
        final CountingOutputStream stream = new CountingOutputStream();
        final long start = System.currentTimeMillis();
        new CypherService(gdb).toJson(result, start, stream);
        final long end = System.currentTimeMillis();
        System.out.println("Streaming " + stream.getCount() + " bytes took " + (end - start) + " ms.");
    }

    private static class ConstantIterator<T> implements Iterator<T> {
        int current;
        private final T data;
        int count;

        public ConstantIterator(T data, int count) {
            this.data = data;
            this.count = count;
        }

        @Override
        public boolean hasNext() {
            return current < count;
        }

        @Override
        public T next() {
            current++;
            return data;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class CountingOutputStream extends OutputStream {
        private int count;

        @Override
        public void write(int b) throws IOException {
            count++;
        }

        public int getCount() {
            return count;
        }
    }

    private class ExecutionResultStub extends ExecutionResult {
        final List<String> columns;
        final Map<String, Object> row;
        private final int count;

        public ExecutionResultStub(final List<String> columns, final Map<String, Object> row, final int count) {
            super(null);
            this.columns = columns;
            this.row = row;
            this.count = count;
        }

        @Override
        public Iterator<Map<String, Object>> iterator() {
            return new ConstantIterator<Map<String, Object>>(row, count);
        }

        @Override
        public List<String> columns() {
            return columns;
        }
    }
}
