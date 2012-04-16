package org.neo4j.server.extension.streaming.cypher.json;

import java.io.IOException;
import java.io.OutputStream;

/**
* @author mh
* @since 16.04.12
*/
public class CountingOutputStream extends OutputStream {
    private int count;

    @Override
    public void write(int b) throws IOException {
        count++;
    }

    public int getCount() {
        return count;
    }
}
