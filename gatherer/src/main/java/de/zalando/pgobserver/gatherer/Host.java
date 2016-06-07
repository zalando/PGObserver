package de.zalando.pgobserver.gatherer;

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

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.pgobserver.gatherer.config.Config;

/**
 * @author  jmussler
 */
public class Host {

    @Override
	public String toString() {
		return "Host [id=" + id + ", name=" + name + ", user=" + user + ", port=" + port + ", dbname=" + dbname
				+ ", uiLongName=" + uiLongName + "]";
	}

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
    // public Map<String, ADBGatherer> gatherersMap = new HashMap<>(); TODO

    private static final Logger LOG = LoggerFactory.getLogger(Host.class);

    public Host() { }

    public HostGatherers getGatherers() {
        return gatherers;
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dbname == null) ? 0 : dbname.hashCode());
		result = prime * result + id;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((password == null) ? 0 : password.hashCode());
		result = prime * result + port;
		result = prime * result + ((settings == null) ? 0 : settings.hashCode());
		result = prime * result + ((settingsAsString == null) ? 0 : settingsAsString.hashCode());
		result = prime * result + ((uiLongName == null) ? 0 : uiLongName.hashCode());
		result = prime * result + ((uiShortName == null) ? 0 : uiShortName.hashCode());
		result = prime * result + ((user == null) ? 0 : user.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Host other = (Host) obj;
		if (dbname == null) {
			if (other.dbname != null)
				return false;
		} else if (!dbname.equals(other.dbname))
			return false;
		if (id != other.id)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (password == null) {
			if (other.password != null)
				return false;
		} else if (!password.equals(other.password))
			return false;
		if (port != other.port)
			return false;
		if (settings == null) {
			if (other.settings != null)
				return false;
		} else if (!settings.equals(other.settings))
			return false;
		if (settingsAsString == null) {
			if (other.settingsAsString != null)
				return false;
		} else if (!settingsAsString.equals(other.settingsAsString))
			return false;
		if (uiLongName == null) {
			if (other.uiLongName != null)
				return false;
		} else if (!uiLongName.equals(other.uiLongName))
			return false;
		if (uiShortName == null) {
			if (other.uiShortName != null)
				return false;
		} else if (!uiShortName.equals(other.uiShortName))
			return false;
		if (user == null) {
			if (other.user != null)
				return false;
		} else if (!user.equals(other.user))
			return false;
		return true;
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

    public static Map<Integer, Host> LoadAllHosts(final Config config) throws SQLException {

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
            throw(se);
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

    public void scheduleGatheres(final Config config) {

        LOG.info("Settings for Host " + getName() + "\n" + "Load: " + settings.getLoadGatherInterval() + " Seconds\n"
                + "Sprocs: " + settings.getSprocGatherInterval() + " Seconds\n" + "Table IO: "
                + settings.getTableIoStatsGatherInterval() + " Seconds\n" + "Table Stats: "
                + settings.getTableStatsGatherInterval() + " Seconds\n" + "Index Stats: "
                + settings.getIndexStatsGatherInterval() + " Seconds\n" + "Schema Stats:"
                + settings.getSchemaStatsGatherInterval() + " Seconds\n" + "Blocking Stats:"
                + settings.getBlockingStatsGatherInterval() + " Seconds\n" + "StatStatements:"
                + settings.getStatStatementsGatherInterval() + " Seconds\n" + "StatDatabase:"
                + settings.getStatDatabaseGatherInterval() + " Seconds\n" + "Bgwriter: "
                + settings.getStatBgwriterGatherInterval() + " Seconds");

        if (gatherers.executor == null) {
            LOG.info("Adding Executor for Host: {}", name);
            gatherers.executor = new ScheduledThreadPoolExecutor(1);
        }

        if (gatherers.sprocGatherer == null) {
            gatherers.sprocGatherer = new SprocGatherer(this, settings.getSprocGatherInterval(), gatherers.executor);
            GathererApp.registerGatherer(gatherers.sprocGatherer);
        } else {
            gatherers.sprocGatherer.setIntervalInSeconds(settings.getSprocGatherInterval());
        }

        if (gatherers.tableStatsGatherer == null) {
            gatherers.tableStatsGatherer = new TableStatsGatherer(this, settings.getTableStatsGatherInterval(),
                    gatherers.executor);
            GathererApp.registerGatherer(gatherers.tableStatsGatherer);
        } else {
            gatherers.tableStatsGatherer.setIntervalInSeconds(settings.getTableStatsGatherInterval());
        }

        if (gatherers.indexStatsGatherer == null) {
            gatherers.indexStatsGatherer = new IndexStatsGatherer(this, settings.getIndexStatsGatherInterval(),
                    gatherers.executor);
            GathererApp.registerGatherer(gatherers.indexStatsGatherer);
        } else {
            gatherers.indexStatsGatherer.setIntervalInSeconds(settings.getIndexStatsGatherInterval());
        }

        if (gatherers.schemaStatsGatherer == null) {
            gatherers.schemaStatsGatherer = new SchemaStatsGatherer(this, settings.getSchemaStatsGatherInterval(),
                    gatherers.executor);
            GathererApp.registerGatherer(gatherers.schemaStatsGatherer);
        } else {
            gatherers.schemaStatsGatherer.setIntervalInSeconds(settings.getSchemaStatsGatherInterval());
        }

        if (gatherers.loadGatherer == null) {
            gatherers.loadGatherer = new LoadGatherer(this, settings.getLoadGatherInterval(), gatherers.executor);
            GathererApp.registerGatherer(gatherers.loadGatherer);
        } else {
            gatherers.loadGatherer.setIntervalInSeconds(settings.getLoadGatherInterval());
        }

        if (gatherers.tableIOStatsGatherer == null) {
            gatherers.tableIOStatsGatherer = new TableIOStatsGatherer(this, settings.getTableIoStatsGatherInterval(),
                    gatherers.executor);
            GathererApp.registerGatherer(gatherers.tableIOStatsGatherer);
        } else {
            gatherers.tableIOStatsGatherer.setIntervalInSeconds(settings.getTableIoStatsGatherInterval());
        }

        if (gatherers.blockingStatsGatherer == null) {
            gatherers.blockingStatsGatherer = new BlockingStatsGatherer(this, settings.getBlockingStatsGatherInterval(),
                    gatherers.executor);
            GathererApp.registerGatherer(gatherers.blockingStatsGatherer);
        } else {
            gatherers.schemaStatsGatherer.setIntervalInSeconds(settings.getBlockingStatsGatherInterval());
        }

        if (gatherers.statStatementsGatherer == null) {
            gatherers.statStatementsGatherer = new StatStatementsGatherer(this,
                    settings.getStatStatementsGatherInterval(), gatherers.executor);
            GathererApp.registerGatherer(gatherers.statStatementsGatherer);
        } else {
            gatherers.statStatementsGatherer.setIntervalInSeconds(settings.getStatStatementsGatherInterval());
        }

        if (gatherers.statDatabaseGatherer == null) {
            gatherers.statDatabaseGatherer = new StatDatabaseGatherer(this, settings.getStatDatabaseGatherInterval(),
                    gatherers.executor);
            GathererApp.registerGatherer(gatherers.statDatabaseGatherer);
        } else {
            gatherers.statDatabaseGatherer.setIntervalInSeconds(settings.getStatDatabaseGatherInterval());
        }

        if (gatherers.bgwriterStatsGatherer == null) {
            gatherers.bgwriterStatsGatherer = new BgwriterStatsGatherer("", this, gatherers.executor,
                    settings.getStatBgwriterGatherInterval());
            GathererApp.registerGatherer(gatherers.bgwriterStatsGatherer);
        }


        if (settings.isSprocGatherEnabled()) {
            LOG.info("Schedule SprocGather for {}", getName());
            gatherers.sprocGatherer.schedule();
        }

        if (settings.isLoadGatherEnabled()) {
            LOG.info("Schedule LoadGather for {}", getName());
            gatherers.loadGatherer.schedule();
        }

        if (settings.isTableIoStatsGatherEnabled()) {
            LOG.info("Schedule TableIO for {}", getName());
            gatherers.tableIOStatsGatherer.schedule();
        }

        if (settings.isTableStatsGatherEnabled()) {
            LOG.info("Schedule TableStats for {}", getName());
            gatherers.tableStatsGatherer.schedule();
        }

        if (settings.isIndexStatsGatherEnabled()) {
            LOG.info("Schedule IndexStats for " + getName());
            gatherers.indexStatsGatherer.schedule();
        }

        if (settings.isSchemaStatsGatherEnabled()) {
            LOG.info("Schedule SchemaStats for " + getName());
            gatherers.schemaStatsGatherer.schedule();
        }

        if (settings.isBlockingStatsGatherEnabled()) {
            LOG.info("Schedule BlockingStats for " + getName());
            gatherers.blockingStatsGatherer.schedule();
        }

        if (settings.isStatStatementsGatherEnabled()) {
            LOG.info("Schedule StatStatement for " + getName());
            gatherers.statStatementsGatherer.schedule();
        }

        if (settings.isStatDatabaseGatherEnabled()) {
            LOG.info("Schedule StatDatabase for " + getName());
            gatherers.statDatabaseGatherer.schedule();
        }

        if (settings.isStatBwriterGatherEnabled()) {
            LOG.info("Schedule Bgwriter for " + getName());
            gatherers.bgwriterStatsGatherer.schedule();
        }
    }

    public void removeGatherers() {
        LOG.info("Removing gatherers for host {}", this.id);
        if (this.gatherers.executor == null)
            return;

        try {
            this.gatherers.executor.shutdown();
            this.gatherers.executor = null;

            // TODO turn into a generic list
            GathererApp.unRegisterGatherer(gatherers.sprocGatherer);
            GathererApp.unRegisterGatherer(gatherers.tableStatsGatherer);
            GathererApp.unRegisterGatherer(gatherers.loadGatherer);
            GathererApp.unRegisterGatherer(gatherers.indexStatsGatherer);
            GathererApp.unRegisterGatherer(gatherers.schemaStatsGatherer);
            GathererApp.unRegisterGatherer(gatherers.blockingStatsGatherer);
            GathererApp.unRegisterGatherer(gatherers.statStatementsGatherer);
            GathererApp.unRegisterGatherer(gatherers.statDatabaseGatherer);
        }
        catch (Exception e) {
            LOG.error(e.getMessage());
        }
        LOG.info("Gatherers successfully removed for host {}", this.id);

    }

}
