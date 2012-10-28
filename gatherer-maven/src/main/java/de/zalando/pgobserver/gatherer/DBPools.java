package de.zalando.pgobserver.gatherer;

import java.sql.Connection;
import java.sql.SQLException;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

/**
 * @author  jmussler
 */
public class DBPools {
    private static BoneCP dataPool = null;

    private static synchronized BoneCP getHostPool() {

        if (dataPool == null) {
            BoneCPConfig config = new BoneCPConfig();
            config.setAcquireIncrement(1);
            config.setJdbcUrl("jdbc:postgresql://localhost/dbmonitor");
            config.setUsername("pgobserver_gatherer");
            config.setPassword("pgobserver_gatherer");
            config.setPartitionCount(1);
            config.setMaxConnectionsPerPartition(10);
            config.setMinConnectionsPerPartition(1);
            config.setConnectionTimeoutInMs(2000);

            try {
                dataPool = new BoneCP(config);
            } catch (SQLException ex) {
                Logger.getLogger(DBPools.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(1);
            }
        }

        return dataPool;
    }

    public static Connection getDataConnection() throws SQLException {
        return getHostPool().getConnection();
    }
}
