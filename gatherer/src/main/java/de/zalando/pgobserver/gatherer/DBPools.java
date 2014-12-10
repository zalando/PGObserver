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

    public static synchronized void initializePool(final Config settings) {
        if (pgObserverDatasource == null) {
            BoneCPConfig config = new BoneCPConfig();
            config.setAcquireIncrement(1);
            config.setJdbcUrl("jdbc:postgresql://" + settings.database.host + ":" + settings.database.port + "/"
                    + settings.database.name);
            config.setUsername(settings.database.backend_user);
            config.setPassword(settings.database.backend_password);
            config.setPartitionCount(1);
            config.setMaxConnectionsPerPartition(20);
            config.setMinConnectionsPerPartition(1);
            config.setConnectionTimeoutInMs(2000);
            config.setInitSQL("set search_path to monitor_data, monitor_api, public");

            pgObserverDatasource = new BoneCPDataSource(config);

            try {

                // check if we can connect to our database
                @SuppressWarnings("unused")
                Connection tryConn = pgObserverDatasource.getConnection();
                tryConn.close();
            } catch (SQLException ex) {
                LOG.error("Error during BoneCP pool creation, exiting", ex);
                System.exit(1);
            }

        }
    }

    public static DataSource getDatasource() {
        return pgObserverDatasource;
    }

    public static Connection getDataConnection() throws SQLException {
        return pgObserverDatasource.getConnection();
    }
}
