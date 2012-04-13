package org.neo4j.server.extension.streaming.cypher;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * @author mh
 * @since 13.04.12
 */
public class CypherResultReader {
    private final JsonFactory jsonFactory;

    public CypherResultReader() {
        this(new JsonFactory(new ObjectMapper()));
    }
    public CypherResultReader(JsonFactory jsonFactory) {
        this.jsonFactory = jsonFactory;
    }

    public static class ResultCallback {
        public void columns(List<String> columns) {
        }

        public void row(int row) {
        }

        public void time(int time) {
        }

        public void count(int count) {
        }

        public void cell(int column, String type, Object value) {
        }
    }

    public void readCypherResults(InputStream inputStream, ResultCallback callback) throws IOException {
        JsonParser jp = jsonFactory.createJsonParser(inputStream);
        nextToken(jp); // will return JsonToken.START_OBJECT (verify?)
        while (nextToken(jp) != JsonToken.END_OBJECT) {
            String field = jp.getCurrentName();
            final JsonToken token = jp.getCurrentToken();
            if (token == JsonToken.FIELD_NAME && field.equals("columns")) { // contains an object
                nextToken(jp); // move to value, or START_OBJECT/START_ARRAY
                callback.columns(jp.readValueAs(List.class));
            }
            if (token == JsonToken.FIELD_NAME && field.equals("rows")) {
                if (nextToken(jp) == JsonToken.START_ARRAY) { // array of rows
                    int row = 0;
                    while (nextToken(jp) != JsonToken.END_ARRAY) { // row
                        callback.row(row++);
                        readRow(jp, callback);
                    }
                }
            }
            if (token == JsonToken.FIELD_NAME && field.equals("time") && nextToken(jp) == JsonToken.VALUE_NUMBER_INT) {
                callback.time(jp.readValueAs(Integer.class));
            }
            if (token == JsonToken.FIELD_NAME && field.equals("count") && nextToken(jp) == JsonToken.VALUE_NUMBER_INT) {
                callback.count(jp.readValueAs(Integer.class));
            }
        }
        jp.close();
    }

    private void readRow(JsonParser jp, ResultCallback callback) throws IOException {
        int column = 0;
        while (nextToken(jp) != JsonToken.END_ARRAY) { // row
            final Map<String, Object> cell = jp.readValueAs(Map.class);
            final Map.Entry<String, Object> inner = cell.entrySet().iterator().next();
            callback.cell(column++, inner.getKey(), inner.getValue());
        }
    }

    private JsonToken nextToken(JsonParser jp) throws IOException {
        final JsonToken jsonToken = jp.nextToken();
        //System.out.println("jsonToken = " + jsonToken);
        return jsonToken;
    }

}
