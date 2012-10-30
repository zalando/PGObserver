package de.zalando.pgobserver.gatherer;

import java.sql.Connection;
import java.sql.SQLException;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import de.zalando.pgobserver.gatherer.config.Config;

/**
 * @author  jmussler
 */
public class DBPools {
    private static BoneCP dataPool = null;
    
    public static synchronized void initializePool(Config settings) {
        if (dataPool == null) {
            BoneCPConfig config = new BoneCPConfig();
            config.setAcquireIncrement(1);
            config.setJdbcUrl("jdbc:postgresql://"+settings.database.host+":"+settings.database.port+"/"+settings.database.name);
            config.setUsername(settings.database.backend_user);
            config.setPassword(settings.database.backend_password);
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
    }

    private static BoneCP getHostPool() {
        return dataPool;
    }

    public static Connection getDataConnection() throws SQLException {
        return getHostPool().getConnection();
    }
}
