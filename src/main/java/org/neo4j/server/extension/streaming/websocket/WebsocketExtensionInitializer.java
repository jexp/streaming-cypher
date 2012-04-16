package org.neo4j.server.extension.streaming.websocket;

import org.apache.commons.configuration.Configuration;
/*
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
*/
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.NeoServer;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;
import org.neo4j.server.plugins.Injectable;
import org.neo4j.server.plugins.SPIPluginLifecycle;

import java.util.Collection;
import java.util.Collections;

/**
 * @author mh
 * @since 16.04.12
 */
public class WebsocketExtensionInitializer implements SPIPluginLifecycle {
    @Override
    public Collection<Injectable<?>> start(GraphDatabaseService graphDatabaseService, Configuration config) {
        return null;
    }

    @Override
    public void stop() {
    }

    @Override
    public Collection<Injectable<?>> start(NeoServer neoServer) {
        return null;
    }

    /* TODO server upgrade to jetty 8.x
    @Override
    public Collection<Injectable<?>> start(final NeoServer neoServer) {
        final Server jetty = getJetty(neoServer);
        final Configurator configurator = neoServer.getConfigurator();

        final ServletHandler servletHandler = new ServletHandler();
        final String mountpoint = "/command"; //getMyMountpoint(configurator);
        System.out.println("mountpoint = " + mountpoint);
        servletHandler.addServletWithMapping(new ServletHolder(new WebSocketServlet(neoServer.getDatabase().graph)), mountpoint);
        jetty.addHandler(servletHandler);

        return Collections.emptyList();
    }

    private Server getJetty(final NeoServer neoServer) {
        if (neoServer instanceof NeoServerWithEmbeddedWebServer) {
            final NeoServerWithEmbeddedWebServer server = (NeoServerWithEmbeddedWebServer) neoServer;
            return server.getWebServer().getJetty();
        } else {
            throw new IllegalArgumentException("expected NeoServerWithEmbeddedWebServer");
        }
    }
    */

    private String getMyMountpoint(final Configurator configurator) {
        final String packageName = getClass().getPackage().getName();

        for (ThirdPartyJaxRsPackage o : configurator.getThirdpartyJaxRsClasses()) {
            if (o.getPackageName().equals(packageName)) {
                return o.getMountPoint();
            }
        }
        throw new RuntimeException("unable to resolve our mountpoint?");
    }
}
