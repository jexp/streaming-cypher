package org.neo4j.server.extension.streaming.cypher.json;

import org.neo4j.cypher.javacompat.ExecutionResult;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
* @author mh
* @since 14.04.12
*/
public class ExecutionResultStub extends ExecutionResult {
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
