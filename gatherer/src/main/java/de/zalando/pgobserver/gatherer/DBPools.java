package de.zalando.pgobserver.gatherer;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;

import de.zalando.pgobserver.gatherer.config.Config;

/**
 * This class holds the datasource for PgObserver Database. We have a connection pool.
 */
public class DBPools {

    private static BoneCPDataSource pgObserverDatasource = null;

    public static final Logger LOG = LoggerFactory.getLogger(DBPools.class);

    public static synchronized boolean initializePool(final Config settings) {
        LOG.info("Connection to db:{} using user: {}", settings.database.host, settings.database.backend_user);
        if (pgObserverDatasource == null) {
            final BoneCPConfig config = new BoneCPConfig();
            config.setAcquireIncrement(1);
            config.setJdbcUrl("jdbc:postgresql://" + settings.database.host + ":" + settings.database.port + "/"
                    + settings.database.name);
            config.setUsername(settings.database.backend_user);
            config.setPassword(settings.database.backend_password);
            config.setPartitionCount(settings.pool.getPartitions());
            config.setMaxConnectionsPerPartition(settings.pool.getMaxConnectionsPerPartition());
            config.setMinConnectionsPerPartition(settings.pool.getMinConnectionsPerPartition());
            config.setConnectionTimeoutInMs(settings.pool.getConnectionTimeoutMilliSeconds());
            config.setInitSQL("set search_path to monitor_data, monitor_api, public");

            pgObserverDatasource = new BoneCPDataSource(config);

            try {

                // check if we can connect to our database
                final Connection tryConn = pgObserverDatasource.getConnection();
                tryConn.close();
            } catch (SQLException ex) {
                LOG.error("Error during BoneCP pool creation, exiting", ex);
                return false;
            }
        }
        return true;
    }

    public static DataSource getDatasource() {
        return pgObserverDatasource;
    }

    public static Connection getDataConnection() throws SQLException {
        return pgObserverDatasource.getConnection();
    }
}
