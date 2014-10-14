package de.zalando.pgobserver.gatherer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


public class TableStatsGatherer extends ADBGatherer {

    private static final Logger LOG = LoggerFactory.getLogger(TableStatsGatherer.class);

    private static final String gathererName = "TableStatsGatherer";
    private final TableIdCache idCache; // schema,name => pgobserver table id
    private Map<Long, List<TableStatsValue>> valueStore = null; // timestamp => list of table data
    private Map<Integer, TableStatsValue> lastValueStore = new HashMap<>(); // table id => table values ( cache not to write same value twice )

    public TableStatsGatherer(final Host h, final long interval, final ScheduledThreadPoolExecutor ex) {
        super(gathererName, h, ex, interval);
        idCache = new TableIdCache(h);
        valueStore = new TreeMap<>();
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
            List<TableStatsValue> list = valueStore.get(time);
            if (list == null) {
                list = new LinkedList<>();
                valueStore.put(time, list);
            }

            ResultSet rs = st.executeQuery(getTableStatsQuery(host.getSettings().getUseTableSizeApproximation() == 1));
            while (rs.next()) {
                TableStatsValue v = new TableStatsValue();
                v.schema = rs.getString("schemaname");
                v.name = rs.getString("relname");
                v.table_size = rs.getLong("table_size");
                v.index_size = rs.getLong("index_size");
                v.seq_scans = rs.getLong("seq_scan");
                v.index_scans = rs.getLong("idx_scan");
                v.tup_inserted = rs.getLong("n_tup_ins");
                v.tup_updated = rs.getLong("n_tup_upd");
                v.tup_deleted = rs.getLong("n_tup_del");
                v.tup_hot_updated = rs.getLong("n_tup_hot_upd");

                list.add(v);
            }

            rs.close();
            conn.close(); // we close here, because we are done
            conn = null;

            LOG.info("[{0}] finished getting table size data",host.name);

            conn = DBPools.getDataConnection();

            PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO monitor_data.table_size_data(tsd_timestamp, tsd_table_id, tsd_table_size, tsd_index_size, tsd_seq_scans, tsd_index_scans, tsd_tup_ins, tsd_tup_upd, tsd_tup_del, tsd_tup_hot_upd, tsd_host_id)    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

            for (Entry<Long, List<TableStatsValue>> toStore : valueStore.entrySet()) {
                for (TableStatsValue v : toStore.getValue()) {

                    int id = idCache.getId(conn, v.schema, v.name);

                    if (!(id > 0)) {
                        LOG.error("could not retrieve table key " + v);
                        continue;
                    }

                    TableStatsValue lastValue = lastValueStore.get(id);

                    if (lastValue != null) {
                        if (v.isEqualTo(lastValue)) {
                            continue;
                        }
                    }

                    ps.setTimestamp(1, new Timestamp(toStore.getKey()));
                    ps.setInt(2, id);
                    ps.setLong(3, v.table_size);
                    ps.setLong(4, v.index_size);
                    ps.setLong(5, v.seq_scans);
                    ps.setLong(6, v.index_scans);
                    ps.setLong(7, v.tup_inserted);
                    ps.setLong(8, v.tup_updated);
                    ps.setLong(9, v.tup_deleted);
                    ps.setLong(10, v.tup_hot_updated);
                    ps.setLong(11, host.id);

                    ps.execute();
                }
            }

            ps.close();
            conn.close();
            conn = null;

            valueStore.clear();

            return true;
        } catch (SQLException se) {
            LOG.error("",se);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException se) {
                    LOG.error("",se);
                }

            }
        }

        return false;
    }

    public static String getTableStatsQuery(boolean useApproximation) {

        String sql;
        
        if (useApproximation)
             sql = "SELECT ut.schemaname, ut.relname, "
                          + "(c.relpages + coalesce(ctd.relpages,0) + coalesce(cti.relpages, 0))::int8 * 8192 as table_size,"
                          + "(select coalesce(sum(relpages),0) from pg_class ci join pg_index on indexrelid =  ci.oid where indrelid = c.oid)::int8 * 8192 as index_size,"
                          + "seq_scan, seq_tup_read, idx_scan,"
                          + "idx_tup_fetch, n_tup_ins , n_tup_upd , n_tup_del , n_tup_hot_upd"
                     + " FROM pg_stat_user_tables ut"
                     + " JOIN pg_class c ON c.oid = ut.relid"
                     + " LEFT JOIN pg_class ctd ON ctd.oid = c.reltoastrelid"
                     + " LEFT JOIN pg_class cti ON cti.oid = ctd.reltoastidxid;";            
        else
            sql = "SELECT schemaname, relname, pg_table_size(relid) as table_size,"
                          + "pg_indexes_size(relid) as index_size, seq_scan, seq_tup_read, idx_scan,"
                          + "idx_tup_fetch, n_tup_ins , n_tup_upd , n_tup_del , n_tup_hot_upd "
                     + "FROM pg_stat_user_tables;";            

        return sql;
    }
}
