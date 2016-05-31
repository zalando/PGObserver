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


public class StatStatementsGatherer extends ADBGatherer {

    private final String ROW_LIMIT = "500\n";
    private final List<StatStatementsValue> valueStore = new ArrayList<StatStatementsValue>();
    private static final String gathererName = "StatStatementsGatherer";

    public static final Logger LOG = LoggerFactory.getLogger(StatStatementsGatherer.class);

    public StatStatementsGatherer(final Host h, final long interval, final ScheduledThreadPoolExecutor ex) {
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

            StatStatementsValue v = null;

            ResultSet rs = st.executeQuery(getQuery());

            while (rs.next()) {
                v = new StatStatementsValue();

                v.timestamp = rs.getTimestamp("timestamp");
                v.query = rs.getString("query");
                v.calls = rs.getLong("calls");
                v.total_time = rs.getLong("total_time");
                v.blks_read = rs.getLong("blks_read");
                v.blks_written = rs.getLong("blks_written");
                v.temp_blks_read = rs.getLong("temp_blks_read");
                v.temp_blks_written = rs.getLong("temp_blks_written");

                valueStore.add(v);
            }

            rs.close();
            conn.close(); // we close here, because we are done

            conn = null;

            if (!valueStore.isEmpty()) {

                LOG.debug("finished getting StatStatements data " + host.name);

                conn = DBPools.getDataConnection();

                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO monitor_data.stat_statements_data (ssd_timestamp, ssd_host_id, ssd_query, ssd_query_id, ssd_calls,"
                            + " ssd_total_time, ssd_blks_read, ssd_blks_written, ssd_temp_blks_read, ssd_temp_blks_written) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

                while (!valueStore.isEmpty()) {

                    v = valueStore.remove(valueStore.size() - 1);

                    ps.setTimestamp(1, v.timestamp);
                    ps.setInt(2, host.id);
                    ps.setString(3, v.query);
                    ps.setLong(4, getLongHashForQuery(v.query)); // will be provided by 9.4, calculating here for now
                    ps.setLong(5, v.calls);
                    ps.setLong(6, v.total_time);
                    ps.setLong(7, v.blks_read);
                    ps.setLong(8, v.blks_written);
                    ps.setLong(9, v.temp_blks_read);
                    ps.setLong(10, v.temp_blks_written);
                    ps.execute();

                }

                ps.close();
                conn.close();
                conn = null;

                LOG.debug("StatStatements values stored " + host.name);

            } else {
                LOG.error("Could not retrieve StatStatements values " + host.name);
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
//        and not (usename = 'postgres' and upper(query) like 'COPY %') ?
        String sql = "with q_data as (\n" +
                "      select\n" +
                "      now() as timestamp,\n" +
                "      s.query,\n" +
                "      sum(s.calls) as calls,\n" +
                "      round(sum(s.total_time))::int8 as total_time,\n" +
                "      sum(shared_blks_read+local_blks_read) as blks_read,\n" +
                "      sum(shared_blks_written+local_blks_written) as blks_written,\n" +
                "      sum(temp_blks_read) as temp_blks_read,\n" +
                "      sum(temp_blks_written) as temp_blks_written\n" +
                "      from\n" +
                "      zz_utils.get_stat_statements() s\n" +
                "      where\n" +
                "      calls > 1\n" +
                "      and total_time > 0\n" +
                "      and not upper(s.query) like any (array['DEALLOCATE%', 'SET %', 'RESET %', 'BEGIN', 'BEGIN;', 'COMMIT', 'COMMIT;', 'END', 'END;', 'ROLLBACK', 'ROLLBACK;'])\n" +
                "      group by\n" +
                "      query\n" +
                ")\n" +
                "select * from (\n" +
                "      select *\n" +
                "      from q_data\n" +
                "      where total_time > 0\n" +
                "      order by\n" +
                "      total_time desc\n" +
                "      limit\n" +
                ROW_LIMIT +
                ") a\n" +
                "union\n" +
                "select * from (\n" +
                "      select *\n" +
                "      from q_data\n" +
                "      order by\n" +
                "      calls desc\n" +
                "      limit\n" +
                ROW_LIMIT +
                ") a\n" +
                "union\n" +
                "select * from (\n" +
                "      select *\n" +
                "      from q_data\n" +
                "      where blks_read > 0\n" +
                "      order by\n" +
                "      blks_read desc\n" +
                "      limit\n" +
                ROW_LIMIT +
                ") a\n" +
                "union\n" +
                "select * from (\n" +
                "      select *\n" +
                "      from q_data\n" +
                "      where blks_written > 0\n" +
                "      order by\n" +
                "      blks_written desc\n" +
                "      limit\n" +
                ROW_LIMIT +
                ") a\n" +
                "union\n" +
                "select * from (\n" +
                "      select *\n" +
                "      from q_data\n" +
                "      where temp_blks_read > 0\n" +
                "      order by\n" +
                "      temp_blks_read desc\n" +
                "      limit\n" +
                ROW_LIMIT +
                ") a\n" +
                "union\n" +
                "select * from (\n" +
                "      select *\n" +
                "      from q_data\n" +
                "      where temp_blks_written > 0\n" +
                "      order by\n" +
                "      temp_blks_written desc\n" +
                "      limit\n" +
                ROW_LIMIT +
                ") a";
        return sql;
    }

    private long getLongHashForQuery(final String query) {
        return (long) query.hashCode();
    }
}
