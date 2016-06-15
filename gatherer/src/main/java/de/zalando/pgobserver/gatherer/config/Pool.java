package de.zalando.pgobserver.gatherer.config;

public class Pool {
	public Integer partitions                    = 2;
	public Integer maxConnectionsPerPartition    = 20;
	public Integer minConnectionsPerPartition    = 1;
	public Integer connectionTimeoutMilliSeconds = 2000;
	@Override
	public String toString() {
		return "Pool [partitions=" + partitions + ", maxConnectionsPerPartition=" + maxConnectionsPerPartition
				+ ", minConnectionsPerPartition=" + minConnectionsPerPartition + ", connectionTimeoutMilliSeconds="
				+ connectionTimeoutMilliSeconds + "]";
	}
	
}
