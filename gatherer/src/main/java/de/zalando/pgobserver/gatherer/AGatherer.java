package de.zalando.pgobserver.gatherer;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class AGatherer implements Runnable {
    private long lastRunInSeconds = 0;
    private long nextRunInSeconds = 0;
    private long lastRunFinishedInSeconds = 0;
    private long intervalInSeconds = 0;
    private long lastSuccessfullPersistInSeconds = 0;
    private ScheduledThreadPoolExecutor executor = null;
    private String hostName = "";
    private String gathererName = "";
    
    private static final Logger LOG = Logger.getLogger(AGatherer.class.getName());

    @Override
	public String toString() {
		return "AGatherer [intervalInSeconds=" + intervalInSeconds + ", executor=" + executor + ", hostName=" + hostName
				+ ", gathererName=" + gathererName + "]";
	}

	protected abstract boolean gatherData();

    protected AGatherer(final String gathererName,  final String hostName, final ScheduledThreadPoolExecutor executor, final long intervalInSeconds) {
        this.gathererName = gathererName;
        this.hostName = hostName;
        this.intervalInSeconds = intervalInSeconds;
        this.executor = executor;
    }

    @Override
    public void run() {
        nextRunInSeconds = (System.currentTimeMillis() / 1000) + intervalInSeconds;
        lastRunInSeconds = System.currentTimeMillis() / 1000;

        LOG.log(Level.INFO, "[{0}] started {1} after interval {2} s", new Object[]{hostName, gathererName, intervalInSeconds});

        try {
            if ( gatherData() ) {
                markSuccessfullPersistance();
            }
        } catch (Throwable t) {
            LOG.log(Level.SEVERE, "Exception in " + this.toString(), t);
        }

        lastRunFinishedInSeconds = System.currentTimeMillis() / 1000;

        LOG.log(Level.INFO, "[{0}] finished {1} after {2} s", new Object[]{hostName, gathererName, lastRunFinishedInSeconds - lastRunInSeconds});
    }
    
    public void markSuccessfullPersistance() {
        lastSuccessfullPersistInSeconds = System.currentTimeMillis() / 1000;
    }

    public String getHostName() {
        return hostName;
    }

    public String getGathererName() {
        return gathererName;
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
            LOG.log(Level.INFO, "Schedule: Interval {0} for Host: {1}", new Object[]{intervalInSeconds, hostName});
            executor.scheduleAtFixedRate(this, 1, intervalInSeconds, TimeUnit.SECONDS);
        } else {
            LOG.log(Level.INFO, "Interval 0 for Host: {}", hostName);
        }
    }

    public void unschedule() {
        executor.remove(this);
    }
}
