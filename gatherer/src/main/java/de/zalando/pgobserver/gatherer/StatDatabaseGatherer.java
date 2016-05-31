package de.zalando.pgobserver.gatherer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StatDatabaseGatherer extends ADBGatherer {

    private final List<StatDatabaseValue> valueStore = new ArrayList<StatDatabaseValue>();
    private static final String gathererName = "StatDatabaseGatherer";

    public static final Logger LOG = LoggerFactory.getLogger(StatDatabaseGatherer.class);

    public StatDatabaseGatherer(final Host h, final long interval, final ScheduledThreadPoolExecutor ex) {
        super(gathererName, h, ex, interval);
    }

    @Override
    protected boolean gatherData() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:postgresql://" + host.name + ":" + host.port + "/" + host.dbname,
                    host.user, host.password);

            Statement st = conn.createStatement();
            st.execute("SET statement_timeout TO '10s';");

            StatDatabaseValue v = null;

            ResultSet rs = st.executeQuery(getQuery());

            while (rs.next()) {
                v = new StatDatabaseValue();

                v.timestamp = rs.getTimestamp("timestamp");
                v.numbackends = rs.getInt("numbackends");
                v.xact_commit = rs.getLong("xact_commit");
                v.xact_rollback = rs.getLong("xact_rollback");
                v.blks_read = rs.getLong("blks_read");
                v.blks_hit = rs.getLong("blks_hit");
                v.temp_files = rs.getLong("temp_files");
                v.temp_bytes = rs.getLong("temp_bytes");
                v.deadlocks = rs.getLong("deadlocks");
                v.blk_read_time = rs.getLong("blk_read_time");
                v.blk_write_time = rs.getLong("blk_write_time");

                valueStore.add(v);
            }

            rs.close();
            conn.close(); // we close here, because we are done

            conn = null;

            if (!valueStore.isEmpty()) {

                LOG.debug("finished getting StatDatabase data " + host.name);

                conn = DBPools.getDataConnection();

                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO monitor_data.stat_database_data (sdd_timestamp, sdd_host_id, sdd_numbackends, sdd_xact_commit, sdd_xact_rollback, sdd_blks_read,"
                            + " sdd_blks_hit, sdd_temp_files, sdd_temp_bytes, sdd_deadlocks, sdd_blk_read_time, sdd_blk_write_time"
                                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

                while (!valueStore.isEmpty()) {

                    v = valueStore.remove(valueStore.size() - 1);

                    ps.setTimestamp(1, v.timestamp);
                    ps.setInt(2, host.id);
                    ps.setLong(3, v.numbackends);
                    ps.setLong(4, v.xact_commit);
                    ps.setLong(5, v.xact_rollback);
                    ps.setLong(6, v.blks_hit);
                    ps.setLong(7, v.blks_read);
                    ps.setLong(8, v.temp_files);
                    ps.setLong(9, v.temp_bytes);
                    ps.setLong(10, v.deadlocks);
                    ps.setLong(11, v.blk_read_time);
                    ps.setLong(12, v.blk_write_time);

                    ps.execute();

                }

                ps.close();
                conn.close();
                conn = null;

                LOG.debug("StatDatabase values stored " + host.name);

            } else {
                LOG.error("Could not retrieve StatDatabase values " + host.name);
            }

            return true;
        } catch (SQLException se) {
            LOG.error(this.toString(), se);
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    LOG.error(this.toString(), ex);
                }
            }
        }
    }

    public String getQuery() {
        String sql = "select\n" +
                    "  now() as timestamp,\n" +
                    "  numbackends,\n" +
                    "  xact_commit,\n" +
                    "  xact_rollback,\n" +
                    "  blks_read,\n" +
                    "  blks_hit,\n" +
                    "  temp_files,\n" +
                    "  temp_bytes,\n" +
                    "  deadlocks,\n" +
                    "  blk_read_time::int8,\n" +
                    "  blk_write_time::int8\n" +
                    "from\n" +
                    "  pg_stat_database\n" +
                    "where\n" +
                    "  datname = current_database()";
        return sql;
    }

}
