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


public class SprocGatherer extends ADBGatherer {

    private SprocIdCache idCache = null;
    private Map<Long, List<SprocPerfValue>> valueStore = null;
    private Map<Integer, Long> lastValueStore = new HashMap<>();
    private int sprocsRead = 0;
    private int sprocValuesInserted = 0;
    private String schemaFilter = null;
    private static final String gathererName = "SprocGatherer";

    private static final Logger LOG = LoggerFactory.getLogger(SprocGatherer.class);

    public SprocGatherer(final Host h, final long interval, final ScheduledThreadPoolExecutor ex) {
        super(gathererName, h, ex, interval);
        idCache = new SprocIdCache(h);
        valueStore = new TreeMap<>();
    }

    public String getQuery(String schemaFilter) {
        String sql = "SELECT\n" +
                "  schemaname AS schema_name,\n" +
                "  funcname  AS function_name, \n" +
                "  ( select array_to_string(array(select format_type(t,null) from unnest(coalesce(proallargtypes, proargtypes::oid[])) tt (t)),',') ) as func_arguments,\n" +
                "  array_to_string(proargmodes, ',') AS func_argmodes,\n" +
                "  calls,\n" +
                "  self_time,\n" +
                "  total_time, \n" +
                "  ( select count(1) from pg_stat_user_functions ff where ff.funcname = f.funcname and ff.schemaname = f.schemaname ) as count_collisions \n" +
                "FROM\n" +
                "  pg_stat_user_functions f,\n" +
                "  pg_proc\n" +
                "WHERE\n" +
                "  pg_proc.oid = f.funcid\n" +
                "  AND schemaname IN ( select name\n" +
                "                      from ( SELECT nspname, rank() OVER ( PARTITION BY regexp_replace(nspname, E'_api_r[_0-9]+', '', 'i') ORDER BY nspname DESC)\n" +
                "                             FROM pg_namespace\n" +
                "                             WHERE " + schemaFilter + "\n" +
                "                           ) apis ( name, rank )\n" +
                "                      where rank <= 4 )";

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
                list = new LinkedList<>();
                valueStore.put(time, list);
            }
            if (this.schemaFilter == null)
                this.schemaFilter = getSchemasToBeMonitored();

            ResultSet rs = st.executeQuery(getQuery(this.schemaFilter));

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

                LOG.debug(v.toString());

                list.add(v);
            }

            rs.close();
            st.close();
            conn.close(); // we close here, because we are done
            conn = null;

            LOG.info("finished getting stored procedure data " + host.getName());

            conn = DBPools.getDataConnection();

            PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO monitor_data.sproc_performance_data(sp_timestamp, sp_sproc_id, sp_calls, sp_total_time, sp_self_time, sp_host_id) VALUES (?, ?, ?, ?, ?, ?);");

            sprocsRead = 0;
            sprocValuesInserted = 0;

            for (Entry<Long, List<SprocPerfValue>> toStore : valueStore.entrySet()) {
                for (SprocPerfValue v : toStore.getValue()) {                    

                    sprocsRead++;

                    int id = idCache.getId(conn, v);

                    if (!(id > 0)) {
                        LOG.error("could not retrieve stored procedure key " + v);
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
                    ps.setLong(6, host.id);

                    ps.execute();
                    sprocValuesInserted++;

                    lastValueStore.put(id, v.totalCalls);
                }
            }

            ps.close();
            conn.close();
            conn = null;

            valueStore.clear();

            LOG.info("[{}] Sprocs read: {} Sprocs written: {}", getHostName(), sprocsRead, sprocValuesInserted);

            return true;
        } catch (SQLException se) {
            LOG.error(this.toString(),se);
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    LOG.error(this.toString(),ex);
                }
            }
        }
    }

    private String getSchemasToBeMonitored() throws SQLException {
        String retSqlExcludes = "NOT nspname LIKE ANY (array[";
        String retSqlIncludes = " AND nspname LIKE ANY (array[";
        String sql = "select scmc_schema_name_pattern as pattern, scmc_is_pattern_included as is_included from sproc_schemas_monitoring_configuration \n" +
                "where scmc_host_id = 0\n" +
                "and not exists (select 1 from sproc_schemas_monitoring_configuration where scmc_host_id = " + Integer.toString(host.id) + ")\n" +
                "union all\n" +
                "select scmc_schema_name_pattern as pattern, scmc_is_pattern_included as is_included " +
                "from sproc_schemas_monitoring_configuration where scmc_host_id = " + Integer.toString(host.id);

        Connection conn = DBPools.getDataConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql);

        while (rs.next()) {
            if (rs.getBoolean("is_included"))
                retSqlIncludes += "'" + rs.getString("pattern") + "',";
            else
                retSqlExcludes += "'" + rs.getString("pattern") + "',";
        }

        rs.close();
        st.close();
        conn.close();
        conn = null;

        retSqlExcludes = retSqlExcludes.replaceAll(",$", "");
        retSqlIncludes = retSqlIncludes.replaceAll(",$", "");
        LOG.warn(retSqlExcludes + "]::text[])" + retSqlIncludes + "]::text[])");
        return retSqlExcludes + "]::text[])" + retSqlIncludes + "]::text[])";
    }
}