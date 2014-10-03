package de.zalando.pgobserver.gatherer;

import de.zalando.pgobserver.gatherer.config.Config;
import java.io.IOException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.codehaus.jackson.map.ObjectMapper;
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

    public static Map<Integer, Host> LoadAllHosts(Config config) {

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
            ResultSet rs = st.executeQuery("SELECT * FROM monitor_data.hosts WHERE host_enabled = true AND host_gather_group = '"+ config.database.gather_group  +"';");
            while (rs.next()) {
                Host h = new Host();
                h.id = rs.getInt("host_id");
                h.name = rs.getString("host_name");
                h.port = rs.getInt("host_port");
                h.user = rs.getString("host_user");
                h.password = rs.getString("host_password");
                h.dbname = rs.getString("host_db");
                h.settingsAsString = rs.getString("host_settings");

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
                + settings.getTableStatsGatherInterval() + " Seconds\n");

        if (gatherers.executor == null) {
            LOG.info("Adding Executor for Host: {}", name);
            gatherers.executor = new ScheduledThreadPoolExecutor(1);
        }

        if (gatherers.sprocGatherer == null) {
            gatherers.sprocGatherer = new SprocGatherer(this, settings.getSprocGatherInterval(), gatherers.executor, config.default_schema_filter);
        } else {
            gatherers.sprocGatherer.setIntervalInSeconds(settings.getSprocGatherInterval());
        }

        if (gatherers.tableStatsGatherer == null) {
            gatherers.tableStatsGatherer = new TableStatsGatherer(this, settings.getTableStatsGatherInterval(),
                    gatherers.executor);
        } else {
            gatherers.tableStatsGatherer.setIntervalInSeconds(settings.getTableStatsGatherInterval());
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

        GathererApp.registerGatherer(gatherers.sprocGatherer);
        GathererApp.registerGatherer(gatherers.tableStatsGatherer);
        GathererApp.registerGatherer(gatherers.loadGatherer);
        GathererApp.registerGatherer(gatherers.loadGatherer);

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
    }
}
