package de.zalando.pgobserver.gatherer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author  jmussler
 */
public class TableIOStatsGatherer extends ADBGatherer {
    private final TableIdCache idCache;
    private Map<Long, List<TableIOStatsValue>> valueStore = null;

    private Map<Integer, TableIOStatsValue> lastValueStore = new HashMap<Integer, TableIOStatsValue>();

    private static final Logger LOG = Logger.getLogger(TableIOStatsGatherer.class.getName());

    public TableIOStatsGatherer(final Host h, final long interval, final ScheduledThreadPoolExecutor ex) {
        super(h, ex, interval);
        idCache = new TableIdCache(h);
        valueStore = new TreeMap<Long, List<TableIOStatsValue>>();

    }

    @Override
    protected boolean gatherData() {

        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:postgresql://" + host.name + ":" + host.port + "/" + host.dbname,
                    host.user, host.password);

            Statement st = conn.createStatement();
            st.execute("SET statement_timeout TO '15s';");

            long time = System.currentTimeMillis();
            List<TableIOStatsValue> list = valueStore.get(time);
            if (list == null) {
                list = new LinkedList<TableIOStatsValue>();
                valueStore.put(time, list);
            }

            ResultSet rs = st.executeQuery(getQuery());
            while (rs.next()) {
                TableIOStatsValue v = new TableIOStatsValue();
                v.schema = rs.getString("schemaname");
                v.name = rs.getString("relname");
                v.heap_blk_read = rs.getLong("heap_blks_read");
                v.heap_blk_hit = rs.getLong("heap_blks_hit");
                v.index_blk_read = rs.getLong("idx_blks_read");
                v.index_blk_hit = rs.getLong("idx_blks_hit");
                list.add(v);
            }

            rs.close();
            conn.close(); // we close here, because we are done
            conn = null;

            LOG.log(Level.INFO, "[{0}] finished getting table io data",
                host.name);

            conn = DBPools.getDataConnection();

            PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO monitor_data.table_io_data(tio_table_id, tio_timestamp, tio_heap_read, tio_heap_hit, tio_idx_read, tio_idx_hit)    VALUES (?, ?, ?, ?, ?, ?);");

            for (Entry<Long, List<TableIOStatsValue>> toStore : valueStore.entrySet()) {
                for (TableIOStatsValue v : toStore.getValue()) {
                    // Logger.getLogger(SprocGatherer.class.getName()).log(Level.INFO, v.schema + "." + v.name);

                    int id = idCache.getId(conn, v.schema, v.name);

                    if (!(id > 0)) {
                        LOG.log(Level.SEVERE, "\t could not retrieve table's key");
                        continue;
                    }

                    TableIOStatsValue lastValue = lastValueStore.get(id);

                    if (lastValue != null) {
                        if (v.isEqualTo(lastValue)) {
                            continue;
                        }
                    }

                    ps.setTimestamp(2, new Timestamp(toStore.getKey()));
                    ps.setInt(1, id);
                    ps.setLong(3, v.heap_blk_read);
                    ps.setLong(4, v.heap_blk_hit);
                    ps.setLong(5, v.index_blk_read);
                    ps.setLong(6, v.index_blk_hit);

                    ps.execute();
                }
            }

            ps.close();
            conn.close();
            conn = null;

            valueStore.clear();

            return true;
        } catch (SQLException se) {
            LOG.log(Level.SEVERE, "", se);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException se) {
                    LOG.log(Level.SEVERE, "", se);
                }

            }
        }

        return false;
    }

    public String getQuery() {
        String sql =
            "select schemaname , relname , heap_blks_read , heap_blks_hit , idx_blks_read , idx_blks_hit from pg_statio_user_tables;";
        return sql;
    }
}
