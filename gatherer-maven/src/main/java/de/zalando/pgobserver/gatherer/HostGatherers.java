package de.zalando.pgobserver.gatherer;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * @author  jmussler
 */
public class HostGatherers {
    public ScheduledThreadPoolExecutor executor = null;
    public SprocGatherer sprocGatherer = null;
    public TableStatsGatherer tableStatsGatherer = null;
    public LoadGatherer loadGatherer = null;
    public TableIOStatsGatherer tableIOStatsGatherer = null;
}
