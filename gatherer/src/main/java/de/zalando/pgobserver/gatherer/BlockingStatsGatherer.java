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


public class BlockingStatsGatherer extends ADBGatherer {

    private List<BlockingProcessValue> valueStoreBlockingProcess = null;
    private List<BlockingLockValue> valueStoreBlockingLock = null;
    private Timestamp maxBlockingProcessTimestamp;
    private Timestamp maxBlockingLocksTimestamp;
    private static final String gathererName = "BlockingStatsGatherer";
    
    private static final Logger LOG = LoggerFactory.getLogger(BlockingStatsGatherer.class);

    public BlockingStatsGatherer(final Host h, final long interval, final ScheduledThreadPoolExecutor ex) {
        super(gathererName, h, ex, interval);
        valueStoreBlockingProcess = new ArrayList<BlockingProcessValue>();
        valueStoreBlockingLock = new ArrayList<BlockingLockValue>();
    }

    public String getQueryBlockingProcesses(Timestamp lastTimestamp) {
        String sql = "select\n" +
                    "  null as bp_host_id, bp_timestamp, datid, datname, pid, usesysid, usename, application_name, client_addr, client_hostname, \n" +
                    "  client_port, backend_start, xact_start, query_start, state_change, waiting, state, query\n" +
                    "from\n" +
                    "  z_blocking.blocking_processes\n" +
                    "where\n"+
                    "  bp_timestamp > '" + lastTimestamp.toString() + "'\n" +
                    "order by\n" +
                    "  bp_timestamp";

        return sql;
    }
    
    public String getQueryBlockingLocks(Timestamp lastTimestamp) {
        String sql = "with t as (\n" +
                    "select \n" +
                    "*\n" +
                    "from z_blocking.blocking_locks where bl_timestamp > '" + lastTimestamp.toString() + "'\n" +
                    ")\n" +
                    "select * from t where not granted\n" +
                    "union all\n" +
                    "select t1.* \n" +
                    "from t t1\n" +
                    "where \n" +
                    "(t1.granted and exists \n" +
                    " (select 1 from t t2\n" +
                    "  where t2.bl_timestamp=t1.bl_timestamp\n" +
                    "    and t2.pid != t1.pid\n" +
                    "    and  \n" +
                    "    (\n" +
                    "      not t2.granted\n" +
                    "      and \n" +
                    "      (  (t1.transactionid is not null and t1.transactionid = t2.transactionid)\n" +
                    "      or \n" +
                    "      (t1.virtualxid is not null and t1.virtualxid = t2.virtualxid)\n" +
                    "      or (t1.classid is not null and t1.classid  = t2.classid and t1.objid = t2.objid and t1.objsubid = t2.objsubid)\n" +
                    "      or (t1.database is not null and t1.database = t2.database and t1.relation = t2.relation)\n" +
                    "      )\n" +
                    "    )\n" +
                    "  )\n" +
                    ")\n" +
                    "order by bl_timestamp\n";

        return sql;
    }
    
    public String getQueryLastEntries() {
        return "select coalesce(max(bp_timestamp),now()-'3 days'::interval) from monitor_data.blocking_processes where bp_host_id = " + Integer.toString(host.id) + "\n" +
                "union all\n" +
                "select coalesce(max(bl_timestamp),now()-'3 days'::interval) from monitor_data.blocking_locks where bl_host_id = " + Integer.toString(host.id);
    }

    @Override
    public boolean gatherData() {
        Connection conn_host = null, conn_pgo = null;

        try {
            conn_host = DriverManager.getConnection("jdbc:postgresql://" + host.name + ":" + host.port + "/" + host.dbname,
                    host.user, host.password);
            conn_pgo = DBPools.getDataConnection();

            Statement st_host = conn_host.createStatement();
            st_host.execute("SET statement_timeout TO '60s';");
            
            Statement st_max_received = conn_pgo.createStatement();            
            ResultSet rs = st_max_received.executeQuery(getQueryLastEntries()); 
            rs.next();
            maxBlockingProcessTimestamp = rs.getTimestamp(1);
            rs.next();
            maxBlockingLocksTimestamp = rs.getTimestamp(1);
            rs.close();
            st_max_received.close();
            
            rs = st_host.executeQuery(getQueryBlockingProcesses(maxBlockingProcessTimestamp));
            
            while (rs.next()) {
                BlockingProcessValue v = new BlockingProcessValue();
                v.bp_host_id = host.id;
                v.bp_timestamp = rs.getTimestamp("bp_timestamp");
                v.datid = rs.getInt("datid");
                v.datname = rs.getString("datname");
                v.pid = rs.getInt("pid");
                v.usesysid = rs.getInt("usesysid");
                v.usename = rs.getString("usename");
                v.application_name = rs.getString("application_name");
                v.client_addr = rs.getString("client_addr");
                v.client_hostname = rs.getString("client_hostname");
                v.client_port = rs.getInt("client_port");
                v.backend_start = rs.getTimestamp("backend_start");
                v.xact_start = rs.getTimestamp("xact_start");
                v.query_start = rs.getTimestamp("query_start");
                v.state_change = rs.getTimestamp("state_change");
                v.waiting = rs.getBoolean("waiting");
                v.state = rs.getString("state");
                v.query = rs.getString("query");

                LOG.debug(v.toString());

                valueStoreBlockingProcess.add(v);
            }

            rs.close();

            LOG.info("finished getting blocking_processes data " + host.getName());

            if (!valueStoreBlockingProcess.isEmpty()) {


                PreparedStatement ps = conn_pgo.prepareStatement(
                        "INSERT INTO monitor_data.blocking_processes("
                                + "bp_host_id, bp_timestamp, datid, datname, pid, usesysid, usename, application_name, client_addr, "
                                + "client_port, backend_start, xact_start, query_start, state_change, waiting, state, query"
                                + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

                while ( !valueStoreBlockingProcess.isEmpty()) {                    
                                        
                    BlockingProcessValue v = valueStoreBlockingProcess.remove(valueStoreBlockingProcess.size()-1);
                
                    ps.setInt(1, v.bp_host_id);
                    ps.setTimestamp(2, v.bp_timestamp);
                    ps.setInt(3, v.datid);
                    ps.setString(4, v.datname);
                    ps.setInt(5, v.pid);
                    ps.setInt(6, v.usesysid);
                    ps.setString(7, v.usename);
                    ps.setString(8, v.application_name);
                    ps.setString(9, v.client_addr);
                    ps.setInt(10, v.client_port);
                    ps.setTimestamp(11, v.backend_start);
                    ps.setTimestamp(12, v.xact_start);
                    ps.setTimestamp(13, v.query_start);
                    ps.setTimestamp(14, v.state_change);
                    ps.setBoolean(15, v.waiting);
                    ps.setString(16, v.state);
                    ps.setString(17, v.query);
                    ps.execute();
                
                }
            }
            
            LOG.info("Getting blocking_locks data " + host.getName());
            
            ResultSet rs_locks = st_host.executeQuery(getQueryBlockingLocks(maxBlockingLocksTimestamp));
            
            while (rs_locks.next()) {
                BlockingLockValue v = new BlockingLockValue();
                v.bl_host_id = host.id;
                v.bl_timestamp = rs_locks.getTimestamp("bl_timestamp");
                v.locktype = rs_locks.getString("locktype");
                v.database = rs_locks.getInt("database");
                v.relation = rs_locks.getInt("relation");
                v.page = rs_locks.getInt("page");
                v.tuple = rs_locks.getShort("tuple");
                v.virtualxid = rs_locks.getString("virtualxid");
                v.transactionid = rs_locks.getString("transactionid");
                v.classid = rs_locks.getInt("classid");
                v.objid = rs_locks.getInt("objid");
                v.objsubid = rs_locks.getShort("objsubid");
                v.virtualtransaction = rs_locks.getString("virtualtransaction");
                v.pid = rs_locks.getInt("pid");
                v.mode = rs_locks.getString("mode");
                v.granted = rs_locks.getBoolean("granted");
                v.fastpath = rs_locks.getBoolean("fastpath");
                
                LOG.debug(v.toString());

                valueStoreBlockingLock.add(v);
            }

            rs_locks.close();
            st_host.close();           
            conn_host.close(); // we close here, because we are done
            conn_host = null;

            LOG.info("finished getting blocking_locks data " + host.getName());

            if (!valueStoreBlockingLock.isEmpty()) {


                PreparedStatement ps = conn_pgo.prepareStatement(
                        "INSERT INTO monitor_data.blocking_locks(" +
                            "bl_host_id, bl_timestamp, locktype, database, relation, page, tuple, virtualxid, transactionid, classid, \n" +
                            "objid, objsubid, virtualtransaction, pid, mode, granted, fastpath\n" +
                            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

                while ( !valueStoreBlockingLock.isEmpty()) {                    
                                        
                    BlockingLockValue v = valueStoreBlockingLock.remove(valueStoreBlockingLock.size()-1);
                
                    ps.setInt(1,v.bl_host_id);
                    ps.setTimestamp(2,v.bl_timestamp);
                    ps.setString(3,v.locktype);
                    ps.setInt(4,v.database);
                    ps.setInt(5,v.relation);
                    ps.setInt(6,v.page);
                    ps.setShort(7,v.tuple);
                    ps.setString(8,v.virtualxid);
                    ps.setString(9,v.transactionid);
                    ps.setInt(10,v.classid);
                    ps.setInt(11,v.objid);
                    ps.setShort(12,v.objsubid);
                    ps.setString(13,v.virtualtransaction);
                    ps.setInt(14,v.pid);
                    ps.setString(15,v.mode);
                    ps.setBoolean(16,v.granted);
                    ps.setBoolean(17,v.fastpath);                    
                    
                    ps.execute();
                }

                ps.close();

                LOG.debug("Blocking_locks values stored [" + host.name + "]");
                
            } else {
                LOG.debug("No blocking_locks values to save [" + host.name + "]");
            }
            
            conn_pgo.close();
            conn_pgo = null;

            return true;
        } catch (SQLException se) {
            LOG.error(this.toString(), se);
            return false;
        } finally {
            if (conn_host != null) {
                try {
                    conn_host.close();
                } catch (SQLException ex) {
                    LOG.error(this.toString(), ex);
                }
            }
        }
    }

   
}