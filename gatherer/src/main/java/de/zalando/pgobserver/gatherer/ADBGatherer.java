package de.zalando.pgobserver.gatherer;

import java.util.concurrent.ScheduledThreadPoolExecutor;


public abstract class ADBGatherer extends AGatherer {
    protected Host host = null;

    public ADBGatherer(final String gathererName, final Host h, final ScheduledThreadPoolExecutor executor, final long intervalInSeconds) {
        super(gathererName, h.getName(), executor, intervalInSeconds);
        host = h;
    }
}
