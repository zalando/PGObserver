package de.zalando.pgobserver.gatherer;


import de.zalando.pgobserver.gatherer.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class ConfigChecker implements Runnable {
    public static final long CONFIG_CHECK_INTERVAL_SECONDS = 600;    // 10min

    private Map<Integer, Host> hosts;
    private Config config;

    private static final Logger LOG = LoggerFactory.getLogger(ConfigChecker.class);

    public ConfigChecker(Map<Integer, Host> hosts, Config config) {
        this.hosts = hosts;
        this.config = config;
    }

    @Override
    public void run() {
        try {
            Map<Integer, Host> hosts_new = Host.LoadAllHosts(config);

            applyConfigChangesIfAny(hosts_new);

            LOG.info("finished checking new config settings. sleeping for {} s", CONFIG_CHECK_INTERVAL_SECONDS);
        } catch (SQLException se) {
            LOG.error("Skipped ConfigChanges due to Exception", se);
        }
    }

    private void applyConfigChangesIfAny(Map<Integer, Host> hosts_new) {

        // remove hosts that are not active anymore
        List<Integer> toRemove = new LinkedList<>();
        for (Host h : this.hosts.values()) {
            if (!hosts_new.containsKey(h.id)) {
                h.removeGatherers();
                toRemove.add(h.id);
            }
        }
        for (Integer i: toRemove) {
            LOG.info("removing host {} as it is not enabled anymore", i);
            this.hosts.remove(i);
        }

        // add and change
        for (Host h : hosts_new.values()) {
            if (!this.hosts.containsKey(h.id)) {            // new host. add
                this.hosts.put(h.id, h);
                h.scheduleGatheres(config);
                LOG.info("added new host with id = {} to scheduling", h.id);
            }
            else        // check for change in intervals
            {
                HostSettings settings = h.getSettings();
                Host hostOld = this.hosts.get(h.id);
                HostSettings settingsOld = hostOld.getSettings();

                if (!(settings.equals(settingsOld) && h.equals(hostOld))) {
                    LOG.info("change in config values detected for host {}. restarting scheduling", h.id);
                    hostOld.removeGatherers();
                    this.hosts.put(h.id, h);
                    h.scheduleGatheres(config);
                }
            }
        }

    }
}
