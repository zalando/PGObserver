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


public class SchemaStatsGatherer extends ADBGatherer {
    private static final String gathererName = "SchemaStatsGatherer";

    private static final Logger LOG = LoggerFactory.getLogger(
            SchemaStatsGatherer.class);

    private List<SchemaStatsValue> valueStore = null;

    public SchemaStatsGatherer(final Host h, final long interval,
        final ScheduledThreadPoolExecutor ex) {
        super(gathererName, h, ex, interval);
        valueStore = new ArrayList<SchemaStatsValue>();
    }

    public String getQuery() {
        String sql = "with q_sproc_calls as (\n" +
"      select\n" +
"            nspname as schemaname,\n" +
"            coalesce(sum(calls),0) as sproc_calls\n" +
"      from\n" +
"            pg_namespace n\n" +
"            left join\n" +
"            pg_stat_user_functions f on f.schemaname = n.nspname\n" +
"      where  \n" +
"            not nspname like any (array['pg_toast%', 'pg_temp%' ,'pgq%', '%utils%'])\n" +
"            and nspname not in ('pg_catalog', 'information_schema', '_v', 'zz_utils', 'zz_commons')    \n" +
"      group by\n" +
"            nspname\n" +
"),\n" +
"q_table_stats as (\n" +
"      select\n" +
"            nspname as schemaname,\n" +
"            sum(seq_scan) as seq_scan,\n" +
"            sum(idx_scan) as idx_scan,\n" +
"            sum(n_tup_ins) as n_tup_ins,\n" +
"            sum(n_tup_upd) as n_tup_upd,\n" +
"            sum(n_tup_del) as n_tup_del     \n" +
"      from\n" +
"            pg_namespace n\n" +
"            left join\n" +
"            pg_stat_all_tables t on t.schemaname = n.nspname\n" +
"      where\n" +
"            not nspname like any (array['pg_toast%', 'pg_temp%' ,'pgq%'])\n" +
"            and nspname not in ('pg_catalog', 'information_schema', '_v', 'zz_utils', 'zz_commons')\n" +
"      group by\n" +
"            nspname\n" +
") \n" +
"select\n" +
"      now(),  \n" +
"      coalesce(t.schemaname,p.schemaname) as schemaname,\n" +
"      coalesce(t.seq_scan,0) as seq_scan,\n" +
"      coalesce(t.idx_scan,0) as idx_scan,\n" +
"      coalesce(t.n_tup_ins,0) as n_tup_ins,\n" +
"      coalesce(t.n_tup_upd,0) as n_tup_upd,\n" +
"      coalesce(t.n_tup_del,0) as n_tup_del,\n" +
"      coalesce(p.sproc_calls,0) as sproc_calls\n" +
"from\n" +
"      q_table_stats t\n" +
"      full outer join\n" +
"      q_sproc_calls p on p.schemaname = t.schemaname\n" +
"order by\n" +
"      2;";


        return sql;
    }

    @Override public boolean gatherData() {
        Connection conn = null;

        try {
            conn = DriverManager.getConnection("jdbc:postgresql://" +
                    host.name + ":" + host.port + "/" + host.dbname, host.user,
                    host.password);

            Statement st = conn.createStatement();
            st.execute("SET statement_timeout TO '15s';");

            ResultSet rs = st.executeQuery(getQuery());

            while (rs.next()) {
                SchemaStatsValue v = new SchemaStatsValue();
                v.timestamp = rs.getTimestamp("now");
                v.hostId = host.id;
                v.schemaName = rs.getString("schemaname");
                v.sprocCalls = rs.getLong("sproc_calls");
                v.seqScans = rs.getLong("seq_scan");
                v.idxScans = rs.getLong("idx_scan");
                v.tupIns = rs.getLong("n_tup_ins");
                v.tupUpd = rs.getLong("n_tup_upd");
                v.tupDel = rs.getLong("n_tup_del");

                LOG.debug(v.toString());

                valueStore.add(v);
            }

            rs.close();
            st.close();
            conn.close(); // we close here, because we are done
            conn = null;

            LOG.info("finished getting schema usage data " + host.getName());


            if (!valueStore.isEmpty()) {

                conn = DBPools.getDataConnection();

                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO monitor_data.schema_usage_data(" +
                        "sud_timestamp, sud_host_id, sud_schema_name, sud_sproc_calls, sud_seq_scans, sud_idx_scans, sud_tup_ins, sud_tup_upd, sud_tup_del" +
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");

                while (!valueStore.isEmpty()) {

                    SchemaStatsValue v = valueStore.remove(valueStore.size() -
                            1);

                    ps.setTimestamp(1, v.timestamp);
                    ps.setInt(2, host.id);
                    ps.setString(3, v.schemaName);
                    ps.setLong(4, v.sprocCalls);
                    ps.setLong(5, v.seqScans);
                    ps.setLong(6, v.idxScans);
                    ps.setLong(7, v.tupIns);
                    ps.setLong(8, v.tupUpd);
                    ps.setLong(9, v.tupDel);
                    ps.execute();

                }

                ps.close();
                conn.close();
                conn = null;

                LOG.info("Schema usage values stored " + host.name);

            } else {
                LOG.error("Could not retrieve/save schema usage values " +
                    host.name);
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
}
