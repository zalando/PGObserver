package de.zalando.pgobserver.gatherer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.pgobserver.gatherer.config.Config;
import java.io.IOException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author  jmussler
 */
public class Host {

    public int id;
    public String name;
    public String user;
    public String password;
    public int port;
    public String dbname;
    public String uiShortName;
    public String uiLongName;
    public String settingsAsString;
    private HostSettings settings = new HostSettings();
    private final HostGatherers gatherers = new HostGatherers();

    private static final Logger LOG = LoggerFactory.getLogger(Host.class);

    public Host() { }

    public HostGatherers getGatherers() {
        return gatherers;
    }

    public Host(final String n, final String db, final int port, final String user, final String password) {
        name = n;
        dbname = db;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public void changeSettings(final HostSettings s) {
        settings = s;
    }

    public HostSettings getSettings() {
        return settings;
    }

    public String getName() {
        return name + "[" + dbname + "]";
    }

    public static Map<Integer, Host> LoadAllHosts(final Config config) {

        /*
         * host_id serial NOT NULL,
         * host_name text,
         * host_port integer,
         * host_user text,
         * host_password text,
         * host_db text,
         */
        Map<Integer, Host> map = new TreeMap<Integer, Host>();

        Connection conn = null;
        try {
            conn = DBPools.getDataConnection();

            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(
                    "SELECT * FROM monitor_data.hosts WHERE host_enabled = true AND host_gather_group = '"
                        + config.database.gather_group + "';");
            while (rs.next()) {
                Host h = new Host();
                h.id = rs.getInt("host_id");
                h.name = rs.getString("host_name");
                h.port = rs.getInt("host_port");
                h.user = rs.getString("host_user");
                h.password = rs.getString("host_password");
                h.dbname = rs.getString("host_db");
                h.settingsAsString = rs.getString("host_settings");
                h.uiLongName = rs.getString("host_ui_longname");
                h.uiShortName = rs.getString("host_ui_shortname");

                ObjectMapper mapper = new ObjectMapper();
                try {
                    h.settings = mapper.readValue(h.settingsAsString, HostSettings.class);
                } catch (IOException e) {
                    LOG.error("Could not deserialize settings object!", e);
                }

                if (h.id > 0) {
                    map.put(h.id, h);
                }
            }

        } catch (SQLException se) {
            LOG.error("Error during loading of host configuration", se);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    LOG.error("Error during close of connection", e);
                }
            }
        }

        return map;
    }

    public void scheduleGatheres(Config config) {

        LOG.info("Settings for Host " + getName() + "\n" + "Load: " + settings.getLoadGatherInterval() + " Seconds\n"
                + "Sprocs: " + settings.getSprocGatherInterval() + " Seconds\n" + "Table IO: "
                + settings.getTableIoStatsGatherInterval() + " Seconds\n" + "Table Stats: "
                + settings.getTableStatsGatherInterval() + " Seconds\n" + "Index Stats: "
                + settings.getIndexStatsGatherInterval() + " Seconds\n" + "Schema Stats:"
                + settings.getSchemaStatsGatherInterval() + " Seconds\n" + "Blocking Stats:"
                + settings.getBlockingStatsGatherInterval() + " Seconds\n" + "StatStatements:"
                + settings.getStatStatementsGatherInterval() + " Seconds\n" + "StatDatabase:"
                + settings.getStatDatabaseGatherInterval() + " Seconds\n");

        if (gatherers.executor == null) {
            LOG.info("Adding Executor for Host: {}", name);
            gatherers.executor = new ScheduledThreadPoolExecutor(1);
        }

        if (gatherers.sprocGatherer == null) {
            gatherers.sprocGatherer = new SprocGatherer(this, settings.getSprocGatherInterval(), gatherers.executor);
        } else {
            gatherers.sprocGatherer.setIntervalInSeconds(settings.getSprocGatherInterval());
        }

        if (gatherers.tableStatsGatherer == null) {
            gatherers.tableStatsGatherer = new TableStatsGatherer(this, settings.getTableStatsGatherInterval(),
                    gatherers.executor);
        } else {
            gatherers.tableStatsGatherer.setIntervalInSeconds(settings.getTableStatsGatherInterval());
        }

        if (gatherers.indexStatsGatherer == null) {
            gatherers.indexStatsGatherer = new IndexStatsGatherer(this, settings.getIndexStatsGatherInterval(),
                    gatherers.executor);
        } else {
            gatherers.indexStatsGatherer.setIntervalInSeconds(settings.getIndexStatsGatherInterval());
        }

        if (gatherers.schemaStatsGatherer == null) {
            gatherers.schemaStatsGatherer = new SchemaStatsGatherer(this, settings.getSchemaStatsGatherInterval(),
                    gatherers.executor);
        } else {
            gatherers.schemaStatsGatherer.setIntervalInSeconds(settings.getSchemaStatsGatherInterval());
        }

        if (gatherers.loadGatherer == null) {
            gatherers.loadGatherer = new LoadGatherer(this, settings.getLoadGatherInterval(), gatherers.executor);
        } else {
            gatherers.loadGatherer.setIntervalInSeconds(settings.getLoadGatherInterval());
        }

        if (gatherers.tableIOStatsGatherer == null) {
            gatherers.tableIOStatsGatherer = new TableIOStatsGatherer(this, settings.getTableIoStatsGatherInterval(),
                    gatherers.executor);
        } else {
            gatherers.tableIOStatsGatherer.setIntervalInSeconds(settings.getTableIoStatsGatherInterval());
        }

        if (gatherers.blockingStatsGatherer == null) {
            gatherers.blockingStatsGatherer = new BlockingStatsGatherer(this, settings.getBlockingStatsGatherInterval(),
                    gatherers.executor);
        } else {
            gatherers.schemaStatsGatherer.setIntervalInSeconds(settings.getBlockingStatsGatherInterval());
        }

        if (gatherers.statStatementsGatherer == null) {
            gatherers.statStatementsGatherer = new StatStatementsGatherer(this,
                    settings.getStatStatementsGatherInterval(), gatherers.executor);
        } else {
            gatherers.statStatementsGatherer.setIntervalInSeconds(settings.getStatStatementsGatherInterval());
        }

        if (gatherers.statDatabaseGatherer == null) {
            gatherers.statDatabaseGatherer = new StatDatabaseGatherer(this,
                    settings.getStatDatabaseGatherInterval(), gatherers.executor);
        } else {
            gatherers.statDatabaseGatherer.setIntervalInSeconds(settings.getStatDatabaseGatherInterval());
        }

        GathererApp.registerGatherer(gatherers.sprocGatherer);
        GathererApp.registerGatherer(gatherers.tableStatsGatherer);
        GathererApp.registerGatherer(gatherers.loadGatherer);
        GathererApp.registerGatherer(gatherers.indexStatsGatherer);
        GathererApp.registerGatherer(gatherers.schemaStatsGatherer);
        GathererApp.registerGatherer(gatherers.blockingStatsGatherer);
        GathererApp.registerGatherer(gatherers.statStatementsGatherer);
        GathererApp.registerGatherer(gatherers.statDatabaseGatherer);

        if (settings.isSprocGatherEnabled()) {
            LOG.info("Schedule SprocGather for {}", getName());
            gatherers.sprocGatherer.schedule();
        } else {
            gatherers.sprocGatherer.unschedule();
        }

        if (settings.isLoadGatherEnabled()) {
            LOG.info("Schedule LoadGather for {}", getName());
            gatherers.loadGatherer.schedule();
        } else {
            gatherers.loadGatherer.unschedule();
        }

        if (settings.isTableIoStatsGatherEnabled()) {
            LOG.info("Schedule TableIO for {}", getName());
            gatherers.tableIOStatsGatherer.schedule();
        } else {
            gatherers.tableIOStatsGatherer.unschedule();
        }

        if (settings.isTableStatsGatherEnabled()) {
            LOG.info("Schedule TableStats for {}", getName());
            gatherers.tableStatsGatherer.schedule();
        } else {
            gatherers.tableStatsGatherer.unschedule();
        }

        if (settings.isIndexStatsGatherEnabled()) {
            LOG.info("Schedule IndexStats for " + getName());
            gatherers.indexStatsGatherer.schedule();
        } else {
            gatherers.indexStatsGatherer.unschedule();
        }

        if (settings.isSchemaStatsGatherEnabled()) {
            LOG.info("Schedule SchemaStats for " + getName());
            gatherers.schemaStatsGatherer.schedule();
        } else {
            gatherers.schemaStatsGatherer.unschedule();
        }

        if (settings.isBlockingStatsGatherEnabled()) {
            LOG.info("Schedule BlockingStats for " + getName());
            gatherers.blockingStatsGatherer.schedule();
        } else {
            gatherers.blockingStatsGatherer.unschedule();
        }

        if (settings.isStatStatementsGatherEnabled()) {
            LOG.info("Schedule StatStatement for " + getName());
            gatherers.statStatementsGatherer.schedule();
        } else {
            gatherers.statStatementsGatherer.unschedule();
        }

        if (settings.isStatDatabaseGatherEnabled()) {
            LOG.info("Schedule StatDatabase for " + getName());
            gatherers.statDatabaseGatherer.schedule();
        } else {
            gatherers.statDatabaseGatherer.unschedule();
        }
    }
}
