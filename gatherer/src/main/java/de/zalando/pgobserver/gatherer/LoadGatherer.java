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
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.ScheduledThreadPoolExecutor;


public class LoadGatherer extends ADBGatherer {

    // used to store values until storage db is available again
    // could have used linked list for pop, but decided for arraylist due to space reasons in case of prolonged connection problems.

    private final List<LoadStatsValue> valueStore = new ArrayList<>();
    private static final String gathererName = "LoadGatherer";

    public static final Logger LOG = LoggerFactory.getLogger(LoadGatherer.class);

    public LoadGatherer(final Host h, final long interval, final ScheduledThreadPoolExecutor ex) {
        super(gathererName, h, ex, interval);
    }

    /*
     * @param   xLogLocation    result from Postgres pg_current_xlog_location, e.g. 2F1/CDABE000 
     * 
     * @return long int of location converted to megabytes (assuming 1 WAL file = 16MB)
     */
    public static long xLogLocationToMb(String xLogLocation) {
        final int MB_PER_WAL = 16;
        
        if (xLogLocation == null || xLogLocation.equals(""))
            return 0;
                
        String[] splits = xLogLocation.split("/");
        while (splits[1].length() != 8) {    // pg_current_xlog_location can return 0/1644148 or 1/4240
            splits[1] = "0" + splits[1];    // brrr, why doesn't Java have String.pad()?
        }

        long ret = Long.parseLong(splits[0] + splits[1].substring(0, 2), 16) * MB_PER_WAL
                + Long.parseLong(splits[1].substring(2), 16) / (1000*1000) - MB_PER_WAL;
        if (ret < 0) {
            ret = 0;
        }
        
        return ret;
    }
    
    @Override
    protected boolean gatherData() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:postgresql://" + host.name + ":" + host.port + "/" + host.dbname,
                    host.user, host.password);

            Statement st = conn.createStatement();
            st.execute("SET statement_timeout TO '5s';");

            long time = System.currentTimeMillis();

            LoadStatsValue v = null;
            String xlog_location = null;
            
            ResultSet rs = st.executeQuery("SELECT * FROM zz_utils.get_load_average() t( min1, min5, min15 ), pg_current_xlog_location();");

            if (rs.next()) {
                v = new LoadStatsValue(time, Math.round( rs.getFloat("min1") * 100), Math.round(rs.getFloat("min5") * 100),
                        Math.round(rs.getFloat("min15") * 100));
                valueStore.add(v);
                xlog_location = rs.getString("pg_current_xlog_location");
            }

            rs.close();
            conn.close(); // we close here, because we are done

            conn = null;

            if (!valueStore.isEmpty()) {

                LOG.debug("finished getting host load data {}", host.name);

                conn = DBPools.getDataConnection();                

                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO monitor_data.host_load(load_timestamp,load_host_id , load_1min_value, load_5min_value, load_15min_value, xlog_location, xlog_location_mb ) VALUES (?, ?, ?, ?, ?, ?, ?);");

                while ( !valueStore.isEmpty()) {                    
                                        
                    v = valueStore.remove(valueStore.size()-1);
                
                    ps.setTimestamp(1, new Timestamp( v.timestamp ) );
                    ps.setInt(2, host.id);
                    ps.setLong(3, v.load_1min);
                    ps.setLong(4, v.load_5min);
                    ps.setLong(5, v.load_15min);
                    ps.setString(6, xlog_location);
                    ps.setLong(7, xLogLocationToMb(xlog_location));
                    ps.execute();
                
                }

                ps.close();
                conn.close();
                conn = null;

                LOG.debug("Load values stored {}", host.name);
                
            } else {
                LOG.error("Could not retrieve host load values {}", host.name);
            }

            return true;
        } catch (SQLException se) {
            LOG.error("Error during Load gathering" + this.toString(), se);
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    LOG.error("Error closing connection" + this.toString(), ex);
                }
            }
        }
    }
}
