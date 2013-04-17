
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
    private long lastSuccessfullPersist = 0;
    private ScheduledThreadPoolExecutor executor = null;
    private String name = "";

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

        Logger.getLogger(AGatherer.class.getName()).info("[" + name + "] started after interval " + intervalInSeconds
                + " s");

        try {
            if ( gatherData() ) {
                markSuccessfullPersistance();
            }
        } catch (Throwable t) {
            Logger.getLogger(AGatherer.class.getName()).log(Level.SEVERE, "Exception", t);
        }

        lastRunFinishedInSeconds = System.currentTimeMillis() / 1000;

        Logger.getLogger(AGatherer.class.getName()).info("[" + name + "] finished after "
                + (lastRunFinishedInSeconds - lastRunInSeconds) + " s");
    }
    
    public void markSuccessfullPersistance() {
        lastSuccessfullPersist = System.currentTimeMillis();
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
        return lastSuccessfullPersist;
    }

    public void schedule() {
        if (intervalInSeconds > 0) {
            Logger.getLogger(AGatherer.class.getName()).severe("Schedule: Interval " + intervalInSeconds + " for Host: "
                    + name);
            executor.scheduleAtFixedRate(this, 1, intervalInSeconds, TimeUnit.SECONDS);
        } else {
            Logger.getLogger(AGatherer.class.getName()).severe("Interval 0 for Host: " + name);
        }
    }

    public void unschedule() {
        executor.remove(this);
    }
}
