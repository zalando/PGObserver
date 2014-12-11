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

public class IndexStatsGatherer extends ADBGatherer {

    private static final Logger LOG = LoggerFactory.getLogger(IndexStatsGatherer.class);
    
    private static final String gathererName = "IndexStatsGatherer";
    private final IndexIdCache idCache; // schema,name => pgobserver table id
    private Map<Long, List<IndexStatsValue>> valueStore = null; // timestamp => list of table data
    private Map<Integer, IndexStatsValue> lastValueStore = new HashMap<>(); // table id => table values ( cache not to write same value twice )

    public IndexStatsGatherer(final Host h, final long interval, final ScheduledThreadPoolExecutor ex) {
        super(gathererName, h, ex, interval);
        idCache = new IndexIdCache(h);
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
            List<IndexStatsValue> list = valueStore.get(time);
            if (list == null) {
                list = new LinkedList<>();
                valueStore.put(time, list);
            }

            ResultSet rs = st.executeQuery( getIndexStatsQuery(host.getSettings().getUseTableSizeApproximation() == 1));
            while (rs.next()) {
                IndexStatsValue v = new IndexStatsValue();
                v.schema = rs.getString("schemaname");
                v.indexrelname = rs.getString("indexrelname");
                v.relname = rs.getString("relname");
                v.scan = rs.getLong("idx_scan");
                v.tup_read = rs.getLong("idx_tup_read");
                v.tup_fetch = rs.getLong("idx_tup_fetch");
                v.size = rs.getLong("index_size");
                list.add(v);
            }

            rs.close();
            conn.close(); // we close here, because we are done
            conn = null;

            LOG.info("[{}] finished getting index data",host.name);

            conn = DBPools.getDataConnection();

            PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO monitor_data.index_usage_data(iud_timestamp, iud_index_id, iud_size, iud_scan, iud_tup_read, iud_tup_fetch, iud_host_id) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?);");

            for (Entry<Long, List<IndexStatsValue>> toStore : valueStore.entrySet()) {
                for (IndexStatsValue v : toStore.getValue()) {

                    int id = idCache.getId(conn, v.schema, v.relname + "::" + v.indexrelname);

                    if (!(id > 0)) {
                        LOG.error("could not retrieve table key " + v);
                        continue;
                    }

                    IndexStatsValue lastValue = lastValueStore.get(id);

                    if (lastValue != null) {
                        if (v.isEqualTo(lastValue)) {
                            continue;
                        }
                    }

                    ps.setTimestamp(1, new Timestamp(toStore.getKey()));
                    ps.setInt(2, id);
                    ps.setLong(3, v.size);
                    ps.setLong(4, v.scan);
                    ps.setLong(5, v.tup_read);
                    ps.setLong(6, v.tup_fetch);
                    ps.setLong(7, host.id);

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

    public static String getIndexStatsQuery(boolean useApproximation) {
        String sql;
        
        if (useApproximation)
            sql = "SELECT schemaname, relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch, "
                + " (select coalesce(relpages, 0) from pg_class where oid = indexrelid)::int8 * 8192 as index_size"
                + " FROM pg_stat_user_indexes WHERE indexrelname not like E'tmp%' and schemaname not like E'%api\\_r%'";            
        else
            sql = "SELECT schemaname, relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch, pg_table_size(indexrelid) as index_size"
                    + " FROM pg_stat_user_indexes WHERE indexrelname not like E'tmp%' and schemaname not like E'%api\\_r%'";

        return sql;
    }

}
