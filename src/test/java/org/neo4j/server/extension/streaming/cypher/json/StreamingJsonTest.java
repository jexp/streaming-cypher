package org.neo4j.server.extension.streaming.cypher.json;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.extension.streaming.cypher.CypherResultReader;
import org.neo4j.test.ImpermanentGraphDatabase;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import static java.util.Arrays.asList;

/**
 * @author mh
 * @since 13.04.12
 */
public class StreamingJsonTest {

    private static final int MILLION = 1000000;
    private final ImpermanentGraphDatabase gdb = new ImpermanentGraphDatabase();
    private Transaction tx;

    @Before
    public void setUp() {
        tx = gdb.beginTx();
        final Node refNode = gdb.getReferenceNode();
        refNode.setProperty("name", "Peter");
        refNode.setProperty("age", 39);
    }

    @After
    public void tearDown() {
        tx.failure();
        tx.finish();
        gdb.shutdown();
    }

    @Test
    public void testReadStreamingOneMillionNodes() throws IOException, InterruptedException {

        final PipedInputStream inputStream = new PipedInputStream(10 * 1024 * 1024);
        final Thread thread = new Thread() {
            public void run() {
                try {
                    ExecutionResult result = new ExecutionResultStub(asList("node"), MapUtil.map("node", gdb.getReferenceNode()), MILLION);
                    final PipedOutputStream stream = new PipedOutputStream(inputStream);
                    new JsonResultWriters().writeTo(stream).toJson(result, System.currentTimeMillis());
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
        ExecutionResult result = new ExecutionResultStub(asList("node"), MapUtil.map("node", gdb.getReferenceNode()), MILLION);
        final CountingOutputStream stream = new CountingOutputStream();
        final long start = System.currentTimeMillis();
        new JsonResultWriters().writeTo(stream).toJson(result, System.currentTimeMillis());
        final long end = System.currentTimeMillis();
        System.out.println("Streaming " + stream.getCount() + " bytes took " + (end - start) + " ms.");
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

}
