package de.zalando.pgobserver.gatherer;

/**
 * @author  jmussler
 */
public class SprocPerfValue {
    String schema;
    String name;
    String parameters;
    String parameterModes;
    long totalCalls;
    long selfTime;
    long totalTime;
}
