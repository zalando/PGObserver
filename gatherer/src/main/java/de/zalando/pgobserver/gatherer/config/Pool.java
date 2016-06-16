package de.zalando.pgobserver.gatherer.config;

public class Pool {
    private int partitions                    = 1;
    private int maxConnectionsPerPartition    = 20;
    private int minConnectionsPerPartition    = 1;
    private int connectionTimeoutMilliSeconds = 2000;

    public int getPartitions() {
        return partitions;
    }
    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }
    public int getMaxConnectionsPerPartition() {
        return maxConnectionsPerPartition;
    }
    public void setMaxConnectionsPerPartition(int maxConnectionsPerPartition) {
        this.maxConnectionsPerPartition = maxConnectionsPerPartition;
    }
    public int getMinConnectionsPerPartition() {
        return minConnectionsPerPartition;
    }
    public void setMinConnectionsPerPartition(int minConnectionsPerPartition) {
        this.minConnectionsPerPartition = minConnectionsPerPartition;
    }
    public int getConnectionTimeoutMilliSeconds() {
        return connectionTimeoutMilliSeconds;
    }
    public void setConnectionTimeoutMilliSeconds(int connectionTimeoutMilliSeconds) {
        this.connectionTimeoutMilliSeconds = connectionTimeoutMilliSeconds;
    }
	@Override
	public String toString() {
		return "Pool [partitions=" + partitions + ", maxConnectionsPerPartition=" + maxConnectionsPerPartition
				+ ", minConnectionsPerPartition=" + minConnectionsPerPartition + ", connectionTimeoutMilliSeconds="
				+ connectionTimeoutMilliSeconds + "]";
	}
	
}
