package org.neo4j.server.extension.streaming.websocket;

/*
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletMapping;
import org.eclipse.jetty.util.resource.Resource;
*/
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.ImpermanentGraphDatabase;

import javax.servlet.http.HttpServlet;

/**
 * @author mh
 * @since 16.04.12
 */
public class Main
{
  public static void main(String[] arg) throws Exception
  {
    Server server = new Server(8080);

    ServletHandler servletHandler = new ServletHandler();

    GraphDatabaseService gdb = new ImpermanentGraphDatabase();
    servletHandler.addServletWithMapping(new ServletHolder(new WebSocketServlet(gdb)),"/command");
    // todo how to set servlet - init-param for db-location here
    // servletHandler.addServletWithMapping(WebSocketServlet.class,"/command");
    /*
    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setBaseResource(Resource.newClassPathResource("public"));

    DefaultHandler defaultHandler = new DefaultHandler();

    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[] {servletHandler,resourceHandler,defaultHandler});
    */
    server.setHandler(servletHandler);

    server.start();
    server.join();
  }
}