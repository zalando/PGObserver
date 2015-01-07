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

    public static Map<Integer, Host> hosts = null;
    
    public static final Logger LOG =  LoggerFactory.getLogger(GathererApp.class);

    public static void registerGatherer(final AGatherer a) {
        ListOfRunnableChecks.add(a);
    }

    public static void unRegisterGatherer(final AGatherer a) {
        ListOfRunnableChecks.remove(a);
    }


    public static String getEnv(String key, String def) {
        String value = System.getenv(key);
        if(value==null || "".equals(value)) {
            return def;
        }
        return value;
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

        // Make env vars overwrite yaml file, to run via docker without changing config file
        config.database.host = getEnv("PGOBS_HOST", config.database.host);
        config.database.port = Integer.parseInt(getEnv("PGOBS_PORT", "" + config.database.port));
        config.database.name = getEnv("PGOBS_DATABASE", config.database.name);
        config.database.backend_user = getEnv("PGOBS_USER", config.database.backend_user);
        config.database.backend_password = getEnv("PGOBS_PASSWORD", config.database.backend_password);

        LOG.info("Connection to db:{} using user: {}", config.database.host, config.database.backend_user);
        
        DBPools.initializePool(config);

        GathererApp.hosts = Host.LoadAllHosts(config);

        for (Host h : GathererApp.hosts.values()) {
            h.scheduleGatheres(config);
        }

        Thread configCheckerThread = new Thread(new ConfigChecker(GathererApp.hosts, config));
        configCheckerThread.setDaemon(true);
        configCheckerThread.start();
        LOG.info("ConfigChecker thread started with check interval of {}s", ConfigChecker.CONFIG_CHECK_INTERVAL_SECONDS);

        try {
            LOG.info("Starting restlet server");
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

            result += "{ \"host_id\" : \"" + ((ADBGatherer)g).host.id
                    + "\", \"host_name\": \"" + g.getHostName()
                    + "\", \"gatherer_name\": \"" + g.getGathererName()
                    + "\", \"last_run\": " + g.getLastRunFinishedInSeconds()
                    + ", \"run_time\" : " + (g.getLastRunFinishedInSeconds() - g.getLastRunInSeconds())
                    + ", \"next_run\" : " + (g.getNextRunInSeconds())
                    + ", \"last_persist\" : " + ( g.getLastSuccessfullPersist() ) + "} ";
        }

        return "{ \"current_time\" : " + System.currentTimeMillis() / 1000 + " , \"jobs\": [" + result + "] }";
    }

}
