package org.neo4j.server.extension.streaming.cypher;

/**
 * @author mh
 * @since 27.02.11
 */

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.server.database.Database;
import org.neo4j.server.extension.streaming.cypher.json.JsonResultWriter;
import org.neo4j.server.extension.streaming.cypher.json.JsonResultWriters;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;

@Path("/cypher")
public class CypherHttpService {

    private final CypherService service;
    private final JsonResultWriters writers;


    public CypherHttpService(@Context Database database) {
        service = new CypherService(database.graph);
        writers = new JsonResultWriters();

    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response query(final @HeaderParam("Accept") String accept,  @Context final UriInfo uriInfo, final String body) {
        try {
            final Map<String, Object> params = params(body);
            StreamingOutput stream = new StreamingOutput() {
                public void write(OutputStream output) throws IOException, WebApplicationException {
                    try {
                        JsonResultWriter writer = writerFor(accept, output, neoServerBaseUri(uriInfo));
                        service.execute((String) params.get("query"), (Map<String, Object>) params.get("params"), writer);
                        writer.close();
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

    private URI neoServerBaseUri(UriInfo uriInfo) {
        return uriInfo.getBaseUriBuilder().replacePath("/db/data/").build();
    }

    private JsonResultWriter writerFor(String accept, OutputStream output, final URI uri) throws IOException {
        final JsonResultWriter writer = accept.contains(";mode=compat") ? writers.writeCompatTo(output, uri.toString()) : writers.writeTo(output);
        if (accept.contains(";format=pretty")) writer.usePrettyPrinter();
        return writer;
    }

    private Map<String, Object> params(String body) throws IOException {
        final Object params = new ObjectMapper().readValue(body, Object.class);
        if (params instanceof Map) return (Map) params; // todo check keys
        throw new IllegalArgumentException("Invalid input " + body);
    }


}
