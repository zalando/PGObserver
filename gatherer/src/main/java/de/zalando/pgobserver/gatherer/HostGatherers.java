package de.zalando.pgobserver.gatherer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * @author  jmussler
 */
public class HostGatherers {
    public ScheduledThreadPoolExecutor executor = null;
    public SprocGatherer sprocGatherer = null;
    public TableStatsGatherer tableStatsGatherer = null;
    public IndexStatsGatherer indexStatsGatherer = null;
    public LoadGatherer loadGatherer = null;
    public TableIOStatsGatherer tableIOStatsGatherer = null;
    public SchemaStatsGatherer schemaStatsGatherer = null;
    public BlockingStatsGatherer blockingStatsGatherer = null;
    public StatStatementsGatherer statStatementsGatherer = null;
    public StatDatabaseGatherer statDatabaseGatherer = null;
    public ADBGatherer bgwriterStatsGatherer = null;
}
