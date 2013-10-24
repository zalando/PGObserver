
package de.zalando.pgobserver.gatherer;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author  jmussler
 */
public abstract class AGatherer implements Runnable {
    private long lastRunInSeconds = 0;
    private long nextRunInSeconds = 0;
    private long lastRunFinishedInSeconds = 0;
    private long intervalInSeconds = 0;
    private long lastSuccessfullPersistInSeconds = 0;
    private ScheduledThreadPoolExecutor executor = null;
    private String name = "";
    
    private static final Logger LOG = Logger.getLogger(AGatherer.class.getName());

    protected abstract boolean gatherData();

    protected AGatherer(final String name, final ScheduledThreadPoolExecutor executor, final long intervalInSeconds) {
        this.name = name;
        this.intervalInSeconds = intervalInSeconds;
        this.executor = executor;
    }

    @Override
    public void run() {
        nextRunInSeconds = (System.currentTimeMillis() / 1000) + intervalInSeconds;
        lastRunInSeconds = System.currentTimeMillis() / 1000;

        LOG.log(Level.INFO, "[{0}] started after interval {1} s", new Object[]{name, intervalInSeconds});

        try {
            if ( gatherData() ) {
                markSuccessfullPersistance();
            }
        } catch (Throwable t) {
            LOG.log(Level.SEVERE, "Exception", t);
        }

        lastRunFinishedInSeconds = System.currentTimeMillis() / 1000;

        LOG.log(Level.INFO, "[{0}] finished after {1} s", new Object[]{name, lastRunFinishedInSeconds - lastRunInSeconds});
    }
    
    public void markSuccessfullPersistance() {
        lastSuccessfullPersistInSeconds = System.currentTimeMillis() / 1000;
    }

    public String getName() {
        return name;
    }

    public boolean isOk() {
        return nextRunInSeconds > (System.currentTimeMillis() / 1000);
    }

    public void setIntervalInSeconds(final long i) {
        intervalInSeconds = i;
    }

    public long getLastRunInSeconds() {
        return lastRunInSeconds;
    }

    public long getLastRunFinishedInSeconds() {
        return lastRunFinishedInSeconds;
    }

    public long getNextRunInSeconds() {
        return nextRunInSeconds;
    }

    public long getIntervalInSeconds() {
        return intervalInSeconds;
    }
    
    public long getLastSuccessfullPersist() {
        return lastSuccessfullPersistInSeconds;
    }

    public void schedule() {
        if (intervalInSeconds > 0) {
            LOG.log(Level.SEVERE, "Schedule: Interval {0} for Host: {1}", new Object[]{intervalInSeconds, name});
            executor.scheduleAtFixedRate(this, 1, intervalInSeconds, TimeUnit.SECONDS);
        } else {
            LOG.log(Level.SEVERE, "Interval 0 for Host: {0}", name);
        }
    }

    public void unschedule() {
        executor.remove(this);
    }
}
