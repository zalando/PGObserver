package de.zalando.pgobserver.gatherer;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

import de.zalando.pgobserver.gatherer.persistence.GatherSprocService;
import de.zalando.pgobserver.gatherer.persistence.GatherSprocServiceImpl;
import de.zalando.pgobserver.gatherer.persistence.PgoWriterSprocService;
import de.zalando.pgobserver.gatherer.persistence.PgoWriterSprocServiceImpl;

import de.zalando.sprocwrapper.dsprovider.DataSourceProvider;
import de.zalando.sprocwrapper.dsprovider.SingleDataSourceProvider;

public abstract class ADBGatherer extends AGatherer {
    protected Host host = null;
    protected GatherSprocService gatherService = null;
    protected PgoWriterSprocService writerService = null;

    public ADBGatherer(final String gathererName, final Host h, final ScheduledThreadPoolExecutor executor,
            final long intervalInSeconds) {
        super(gathererName, h.getName(), executor, intervalInSeconds);
        host = h;
        initGatherService();
        initWriterService();
    }

    /**
     * initialize the Sproc service gathering data from current host. (which is acutally a database)
     */
    private void initGatherService() {

// DriverManagerDataSource returns a new connection every time that a connection is requested.
// TODO: why must every gatherer has its own connection? what is the cost of create a connection in comparison to a
// persistent connection?
        String url = "jdbc:postgresql://" + host.name + ":" + host.port + "/" + host.dbname;
        DataSource ds = new DriverManagerDataSource(url, host.user, host.password);

        DataSourceProvider datasource = new SingleDataSourceProvider(ds);
        this.gatherService = new GatherSprocServiceImpl(datasource);
    }

    /**
     * initialize the sproc service for writing to the pgobserver database.
     */
    private void initWriterService() {
        DataSourceProvider ds = new SingleDataSourceProvider(DBPools.getDatasource());
        this.writerService = new PgoWriterSprocServiceImpl(ds);
    }

}
