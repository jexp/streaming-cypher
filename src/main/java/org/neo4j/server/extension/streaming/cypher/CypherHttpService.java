package org.neo4j.server.extension.streaming.cypher;

/**
 * @author mh
 * @since 27.02.11
 */

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.server.database.Database;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

@Path("/cypher")
public class CypherHttpService {

    private final CypherService service;


    public CypherHttpService(@Context Database database) {
        service = new CypherService(database.graph);

    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response query(final String body) { // todo params
        try {
            final Map<String, Object> params = params(body);
            StreamingOutput stream = new StreamingOutput() {
                public void write(OutputStream output) throws IOException, WebApplicationException {
                    try {
                        service.execute((String) params.get("query"), (Map<String, Object>) params.get("params"), output);
                    } catch (Exception e) {
                        throw new WebApplicationException(e);
                    }
                }
            };
            return Response.ok(stream).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }

    private Map<String, Object> params(String body) throws IOException {
        final Object params = new ObjectMapper().readValue(body, Object.class);
        if (params instanceof Map) return (Map) params; // todo check keys
        throw new IllegalArgumentException("Invalid input " + body);
    }


}
