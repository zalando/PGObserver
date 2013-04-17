package de.zalando.pgobserver.gatherer;

import de.zalando.pgobserver.gatherer.config.Config;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.restlet.Server;

import org.restlet.data.Protocol;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

/**
 * @author  jmussler
 */
public class GathererApp extends ServerResource {

    public static final List<AGatherer> ListOfRunnableChecks = new LinkedList<AGatherer>();
    
    public static final Logger LOG =  Logger.getLogger(GathererApp.class.getName());

    public static void registerGatherer(final AGatherer a) {
        ListOfRunnableChecks.add(a);
    }

    /**
     * @param  args  the command line arguments
     */
    public static void main(final String[] args) {
        
        Config config = Config.LoadConfigFromFile(System.getProperty("user.home") + "/.pgobserver.conf");
        
        System.out.println(config);
        
        if ( config == null ) {
            LOG.warning("Configfile could not be read");
            return;
        }
        
        System.out.println("Connection to db:" + config.database.host + " using user: " + config.database.backend_user );
        
        DBPools.initializePool(config);
        
        Map<Integer, Host> hosts = Host.LoadAllHosts(config);

        for (Host h : hosts.values()) {
            h.scheduleGatheres();
        }

        try {
            new Server(Protocol.HTTP, 8182, GathererApp.class).start();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
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
