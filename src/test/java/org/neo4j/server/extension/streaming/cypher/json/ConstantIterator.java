package org.neo4j.server.extension.streaming.cypher.json;

import java.util.Iterator;

/**
* @author mh
* @since 14.04.12
*/
public class ConstantIterator<T> implements Iterator<T> {
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
