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
public class SprocGatherer extends ADBGatherer {

    public static final int QueryID = 1;
    private SprocIdCache idCache = null;
    private Map<Long, List<SprocPerfValue>> valueStore = null;
    private Map<Integer, Long> lastValueStore = new HashMap<Integer, Long>();
    private int sprocsRead = 0;
    private int sprocValuesInserted = 0;
    
    public static final Logger LOG = Logger.getLogger(SprocGatherer.class.getName());

    public SprocGatherer(final Host h, final long interval, final ScheduledThreadPoolExecutor ex) {
        super(h, ex, interval);
        idCache = new SprocIdCache(h);
        valueStore = new TreeMap<Long, List<SprocPerfValue>>();
    }

    public String getQuery() {
        String sql = "SELECT schemaname AS schema_name,"
                + "funcname  AS function_name, "
                + "(SELECT array_to_string(ARRAY(SELECT format_type(t,null) FROM unnest(proallargtypes) tt ( t ) ),',')) AS func_arguments,"
                + "array_to_string(proargmodes,',') AS func_argmodes,"
                + "calls, self_time, total_time, "
                + "(SELECT count(1) FROM pg_stat_user_functions ff WHERE ff.funcname = f.funcname AND ff.schemaname = f.schemaname) AS count_collisions "
                + "FROM pg_stat_user_functions f, pg_proc "
                + "WHERE pg_proc.oid = f.funcid and not schemaname like any( array['pg%','information_schema'] ) "
                + "AND ( schemaname IN ( SELECT name FROM ( SELECT nspname, rank() OVER ( PARTITION BY substring(nspname from '(.*)_api') ORDER BY nspname DESC) FROM pg_namespace WHERE nspname LIKE '%_api%' ) apis ( name, rank ) where rank = 1 ) OR schemaname LIKE '%_data' );";

        return sql;
    }

    @Override
    public boolean gatherData() {
        Connection conn = null;

        try {
            conn = DriverManager.getConnection("jdbc:postgresql://" + host.name + ":" + host.port + "/" + host.dbname,
                    host.user, host.password);

            Statement st = conn.createStatement();
            st.execute("SET statement_timeout TO '15s';");

            long time = System.currentTimeMillis();
            List<SprocPerfValue> list = valueStore.get(time);
            if (list == null) {
                list = new LinkedList<SprocPerfValue>();
                valueStore.put(time, list);
            }

            ResultSet rs = st.executeQuery(getQuery());
            while (rs.next()) {
                SprocPerfValue v = new SprocPerfValue();
                v.name = rs.getString("function_name");
                v.schema = rs.getString("schema_name");
                v.parameters = rs.getString("func_arguments");
                v.parameterModes = rs.getString("func_argmodes");
                v.selfTime = rs.getLong("self_time");
                v.totalCalls = rs.getLong("calls");
                v.totalTime = rs.getLong("total_time");
                v.collisions = rs.getInt("count_collisions");
                list.add(v);
            }

            rs.close();
            st.close();
            conn.close(); // we close here, because we are done
            conn = null;

            Logger.getLogger(SprocGatherer.class.getName()).log(Level.INFO, "[{0}] finished getting sproc data",
                host.name);

            conn = DBPools.getDataConnection();

            PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO monitor_data.sproc_performance_data(sp_timestamp, sp_sproc_id, sp_calls, sp_total_time, sp_self_time) VALUES (?, ?, ?, ?, ?);");

            sprocsRead = 0;
            sprocValuesInserted = 0;

            for (Entry<Long, List<SprocPerfValue>> toStore : valueStore.entrySet()) {
                for (SprocPerfValue v : toStore.getValue()) {                    

                    sprocsRead++;

                    int id = idCache.getId(conn, v);

                    if (!(id > 0)) {
                        Logger.getLogger(SprocGatherer.class.getName()).log(Level.SEVERE,
                            "could not retrieve sproc key");
                        continue;
                    }

                    Long lastValue = lastValueStore.get(id);

                    if (lastValue != null) {
                        if (lastValue == v.totalCalls) {
                            continue;
                        }
                    }

                    ps.setTimestamp(1, new Timestamp(toStore.getKey()));
                    ps.setInt(2, id);
                    ps.setLong(3, v.totalCalls);
                    ps.setLong(4, v.totalTime);
                    ps.setLong(5, v.selfTime);

                    ps.execute();
                    sprocValuesInserted++;

                    lastValueStore.put(id, v.totalCalls);
                }
            }

            ps.close();
            conn.close();
            conn = null;

            valueStore.clear();

            LOG.log(Level.INFO, "[{0}] Sprocs read: {1} Sprocs written: {2}", new Object[]{this.getName(), this.sprocsRead, this.sprocValuesInserted});

            return true;
        } catch (SQLException se) {
            LOG.log(Level.SEVERE, "", se);
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    LOG.log(Level.SEVERE, "", ex);
                }
            }
        }
    }
}