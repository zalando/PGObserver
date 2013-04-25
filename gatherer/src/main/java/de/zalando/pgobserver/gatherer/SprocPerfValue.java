package de.zalando.pgobserver.gatherer;

/**
 * @author  jmussler
 */
public class SprocPerfValue {
    String schema;
    String name;
    long totalCalls;
    long selfTime;
    long totalTime;
}
