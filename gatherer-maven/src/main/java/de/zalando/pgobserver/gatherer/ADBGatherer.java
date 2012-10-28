package de.zalando.pgobserver.gatherer;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * @author  jmussler
 */
public abstract class ADBGatherer extends AGatherer {
    protected Host host = null;

    public ADBGatherer(final Host h, final ScheduledThreadPoolExecutor executor, final long intervalInSeconds) {
        super(h.getName(), executor, intervalInSeconds);
        host = h;
    }
}
