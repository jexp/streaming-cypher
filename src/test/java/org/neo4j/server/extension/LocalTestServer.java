package org.neo4j.server.extension;

import org.apache.commons.configuration.Configuration;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.Bootstrapper;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.configuration.EmbeddedServerConfigurator;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.GraphDatabaseFactory;
import org.neo4j.server.extension.streaming.cypher.CypherHttpService;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.modules.ThirdPartyJAXRSModule;
import org.neo4j.server.startup.healthcheck.StartupHealthCheck;
import org.neo4j.server.startup.healthcheck.StartupHealthCheckRule;
import org.neo4j.server.web.Jetty6WebServer;
import org.neo4j.test.ImpermanentGraphDatabase;

import java.net.URI;
import java.util.*;

/**
 * @author mh
 * @since 24.03.11
 */
public class LocalTestServer {
    private NeoServerWithEmbeddedWebServer neoServer;
    private final int port;
    private final String hostname;

    public LocalTestServer() {
        this("localhost", 7473);
    }

    public LocalTestServer(String hostname, int port) {
        this.port = port;
        this.hostname = hostname;
    }

    public void start() {
        if (neoServer != null) throw new IllegalStateException("Server already running");
        final List<Class<? extends ServerModule>> serverModules = Arrays.<Class<? extends ServerModule>>asList((Class<? extends ServerModule>)ThirdPartyJAXRSModule.class);
        final ImpermanentGraphDatabase gdb = new ImpermanentGraphDatabase();
        final Bootstrapper bootstrapper = new Bootstrapper() {
            @Override
            protected GraphDatabaseFactory getGraphDatabaseFactory(Configuration configuration) {
                return new GraphDatabaseFactory() {
                    @Override
                    public AbstractGraphDatabase createDatabase(String databaseStoreDirectory, Map<String, String> databaseProperties) {
                        return gdb;
                    }
                };
            }

            @Override
            protected Iterable<StartupHealthCheckRule> getHealthCheckRules() {
                return Collections.emptyList();
            }

            @Override
            protected Iterable<Class<? extends ServerModule>> getServerModules() {
                return serverModules;
            }
        };
        neoServer = new NeoServerWithEmbeddedWebServer(bootstrapper
                , new StartupHealthCheck(), new EmbeddedServerConfigurator(gdb) {
            @Override
            public Set<ThirdPartyJaxRsPackage> getThirdpartyJaxRsClasses() {
                return Collections.singleton(new ThirdPartyJaxRsPackage(CypherHttpService.class.getPackage().getName(),"/streaming"));
            }
        }, new Jetty6WebServer(), serverModules) {
            @Override
            protected int getWebServerPort() {
                return port;
            }
        };
        neoServer.start();
    }

    public void stop() {
        try {
            neoServer.stop();
        } catch (Exception e) {
            System.err.println("Error stopping server: " + e.getMessage());
        }
        neoServer = null;
    }

    public int getPort() {
        return port;
    }

    public String getHostname() {
        return hostname;
    }

    public Database getDatabase() {
        return neoServer.getDatabase();
    }

    public URI baseUri() {
        return neoServer.baseUri();
    }

    public GraphDatabaseService getGraphDatabase() {
        return getDatabase().graph;
    }
}
