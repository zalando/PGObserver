package de.zalando.pgobserver.gatherer;


import de.zalando.pgobserver.gatherer.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class ConfigChecker implements Runnable {
    private static final long CONFIG_CHECK_INTERVAL_MILLIS = 300000;    // 5min

    private Map<Integer, Host> hosts;
    private Config config;

    private static final Logger LOG = LoggerFactory.getLogger(SprocGatherer.class);

    public ConfigChecker(Map<Integer, Host> hosts, Config config) {
        this.hosts = hosts;
        this.config = config;
    }

    @Override
    public void run() {
        while (true) {

            try {
                Thread.sleep(CONFIG_CHECK_INTERVAL_MILLIS);

                Map<Integer, Host> hosts_new = Host.LoadAllHosts(config);

                applyConfigChangesIfAny(hosts_new);

                LOG.error("finished checking new config settings. sleeping for {} ms", CONFIG_CHECK_INTERVAL_MILLIS);
            } catch (InterruptedException ie) {
                LOG.error("", ie);
            }
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
                Boolean changeDetected = false;
                HostSettings settings = h.getSettings();
                HostSettings settingsOld = this.hosts.get(h.id).getSettings();

                if (settings.getBlockingStatsGatherInterval() != settingsOld.getBlockingStatsGatherInterval()
                        || settings.getIndexStatsGatherInterval() != settingsOld.getIndexStatsGatherInterval()
                        || settings.getLoadGatherInterval() != settingsOld.getLoadGatherInterval()
                        || settings.getSchemaStatsGatherInterval() != settingsOld.getSchemaStatsGatherInterval()
                        || settings.getSprocGatherInterval() != settingsOld.getSprocGatherInterval()
                        || settings.getStatBgwriterGatherInterval() != settingsOld.getStatBgwriterGatherInterval()
                        || settings.getStatDatabaseGatherInterval() != settingsOld.getStatDatabaseGatherInterval()
                        || settings.getStatStatementsGatherInterval() != settingsOld.getStatStatementsGatherInterval()
                        || settings.getTableIoStatsGatherInterval() != settingsOld.getTableIoStatsGatherInterval()
                        || settings.getTableStatsGatherInterval() != settingsOld.getTableStatsGatherInterval()
                        || settings.getUseTableSizeApproximation() != settingsOld.getUseTableSizeApproximation()
                        )
                    changeDetected = true;

                if (changeDetected) {
                    LOG.info("change in config values detected for host {}. restarting scheduling", h.id);
                    this.hosts.get(h.id).removeGatherers();
                    this.hosts.put(h.id, h);
                    h.scheduleGatheres(config);
                }
            }
        }

    }
}
