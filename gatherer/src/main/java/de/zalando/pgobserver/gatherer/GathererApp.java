package de.zalando.pgobserver.gatherer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import de.zalando.pgobserver.gatherer.config.Config;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.restlet.Server;

import org.restlet.data.Protocol;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author  jmussler
 */
public class GathererApp extends ServerResource {

    public static final List<AGatherer> ListOfRunnableChecks = new LinkedList<>();
    
    public static final Logger LOG =  LoggerFactory.getLogger(GathererApp.class);

    public static void registerGatherer(final AGatherer a) {
        ListOfRunnableChecks.add(a);
    }

    /**
     * @param  args  the command line arguments
     */
    public static void main(final String[] args) {

        Config config;

        config = Config.LoadConfigFromFile(new ObjectMapper(new YAMLFactory()), System.getProperty("user.home") + "/.pgobserver.yaml");
        if ( config == null ) {
            LOG.error("Config could not be read from yaml");
            return;
        }

        LOG.info("Connection to db:{} using user: {}", config.database.host, config.database.backend_user);
        
        DBPools.initializePool(config);
        
        Map<Integer, Host> hosts = Host.LoadAllHosts(config);

        for (Host h : hosts.values()) {
            h.scheduleGatheres(config);
        }

        try {
            new Server(Protocol.HTTP, 8182, GathererApp.class).start();
        } catch (Exception ex) {
            LOG.error("Could not start restlet server", ex);
        }
    }

    @Get
    public String overview() {
        String result = "";
        for (AGatherer g : ListOfRunnableChecks) {
            if (!result.equals("")) {
                result += ",";
            }

            result += "{ \"name\": \"" + g.getName() + "\", \"last_run\": " + g.getLastRunFinishedInSeconds()
                    + ", \"run_time\" : " + (g.getLastRunFinishedInSeconds() - g.getLastRunInSeconds())
                    + ", \"next_run\" : " + (g.getNextRunInSeconds())
                    + ", \"last_persist\" : " + ( g.getLastSuccessfullPersist() ) + "} ";
        }

        return "{ \"current_time\" : " + System.currentTimeMillis() / 1000 + " , \"jobs\": [" + result + "] }";
    }
}
