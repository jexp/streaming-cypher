package org.neo4j.server.extension.streaming.cypher;

import java.util.List;

/**
 * @author mh
 * @since 13.04.12
 */
public class PrintingResultCallback extends CypherResultReader.ResultCallback {
    @Override
    public void columns(List<String> columns) {
        System.out.println("columns = " + columns);
    }

    @Override
    public void row(int row) {
        System.out.println(row+".");
    }

    @Override
    public void cell(int column, String type, Object value) {
        System.out.printf("\t%d. %s -> %s%n", column, type, value);
    }

    @Override
    public void time(int time) {
        System.out.println("Took "+time+" ms.");
    }

    @Override
    public void count(int count) {
        System.out.println(count+" Rows");
    }
}
