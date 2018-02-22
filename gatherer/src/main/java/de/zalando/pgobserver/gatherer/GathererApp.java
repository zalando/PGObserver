package de.zalando.pgobserver.gatherer;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.restlet.Server;
import org.restlet.data.Protocol;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zalando.pgobserver.gatherer.config.Config;

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

        final String configFileName = getConfigFileName(args);

        final Config config = Config.LoadConfigFromFile(configFileName);
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

        if (!DBPools.initializePool(config)){
            return;
        }

        GathererApp.hosts = new TreeMap<Integer, Host>(); 
        ScheduledExecutorService configCheckService = Executors.newScheduledThreadPool(1);
        ConfigChecker c = new ConfigChecker(GathererApp.hosts, config);
        configCheckService.scheduleAtFixedRate(c, 0, ConfigChecker.CONFIG_CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
        LOG.info("ConfigChecker thread started with check interval of {}s", ConfigChecker.CONFIG_CHECK_INTERVAL_SECONDS);

        try {
            LOG.info("Starting restlet server");
            new Server(Protocol.HTTP, Integer.parseInt(getEnv("HTTP_PORT", "8182")), GathererApp.class).start();
        } catch (Exception ex) {
            LOG.error("Could not start restlet server", ex);
        }
    }

    private static String getConfigFileName(final String[] args) {

        if (args.length == 0) {
            return System.getProperty("user.home") + "/.pgobserver.yaml";
        } else if (args.length == 1) {
            return args[0];
        } else {
            LOG.error("Too many arguments on command line");
            LOG.error("usage: gatherer [CONFIG_FILE]");
            return null;
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
