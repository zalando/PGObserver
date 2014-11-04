package de.zalando.pgobserver.gatherer.domain;

import java.util.Date;

import de.zalando.typemapper.annotations.DatabaseField;

/**
 * This class represents the data of the background writer statistics for a database cluster. The columns of
 * pg_stat_bgwriter view are enriched with host and current timestamp.
 *
 * @author  slitsche
 */

public class BgwriterValue {
    @DatabaseField
    int hostId;
    @DatabaseField
    Date sbTimestamp;
    @DatabaseField
    long checkpointsTimed;
    @DatabaseField
    long checkpointsReq;
    @DatabaseField
    double checkpointWriteTime;
    @DatabaseField
    double checkpointSyncTime;
    @DatabaseField
    long buffersCheckpoint;
    @DatabaseField
    long buffersClean;
    @DatabaseField
    long maxwrittenClean;
    @DatabaseField
    long buffersBackend;
    @DatabaseField
    long buffersBackendFsync;
    @DatabaseField
    long buffersAlloc;
    @DatabaseField
    Date statsReset;

    public int getHostId() {
        return hostId;
    }

    public void setHostId(final int hostId) {
        this.hostId = hostId;
    }

    public Date getSbTimestamp() {
        return sbTimestamp;
    }

    public void setSbTimestamp(final Date sbTimestamp) {
        this.sbTimestamp = sbTimestamp;
    }

    public long getCheckpointsTimed() {
        return checkpointsTimed;
    }

    public void setCheckpointsTimed(final long checkpointsTimed) {
        this.checkpointsTimed = checkpointsTimed;
    }

    public long getCheckpointsReq() {
        return checkpointsReq;
    }

    public void setCheckpointsReq(final long checkpointsReq) {
        this.checkpointsReq = checkpointsReq;
    }

    public double getCheckpointWriteTime() {
        return checkpointWriteTime;
    }

    public void setCheckpointWriteTime(final double checkpointWriteTime) {
        this.checkpointWriteTime = checkpointWriteTime;
    }

    public double getCheckpointSyncTime() {
        return checkpointSyncTime;
    }

    public void setCheckpointSyncTime(final double checkpointSyncTime) {
        this.checkpointSyncTime = checkpointSyncTime;
    }

    public long getBuffersCheckpoint() {
        return buffersCheckpoint;
    }

    public void setBuffersCheckpoint(final long buffersCheckpoint) {
        this.buffersCheckpoint = buffersCheckpoint;
    }

    public long getBuffersClean() {
        return buffersClean;
    }

    public void setBuffersClean(final long buffersClean) {
        this.buffersClean = buffersClean;
    }

    public long getMaxwrittenClean() {
        return maxwrittenClean;
    }

    public void setMaxwrittenClean(final long maxwrittenClean) {
        this.maxwrittenClean = maxwrittenClean;
    }

    public long getBuffersBackend() {
        return buffersBackend;
    }

    public void setBuffersBackend(final long buffersBackend) {
        this.buffersBackend = buffersBackend;
    }

    public long getBuffersBackendFsync() {
        return buffersBackendFsync;
    }

    public void setBuffersBackendFsync(final long buffersBackendFsync) {
        this.buffersBackendFsync = buffersBackendFsync;
    }

    public long getBuffersAlloc() {
        return buffersAlloc;
    }

    public void setBuffersAlloc(final long buffersAlloc) {
        this.buffersAlloc = buffersAlloc;
    }

    public Date getStatsReset() {
        return statsReset;
    }

    public void setStatsReset(final Date statsReset) {
        this.statsReset = statsReset;
    }

}
