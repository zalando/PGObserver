package de.zalando.pgobserver.gatherer;

/**
 * @author  jmussler
 */
public class SprocPerfValue {
    String schema;
    String name;
    String parameters;
    String parameterModes;
    int collisions;
    long totalCalls;
    long selfTime;
    long totalTime;

    @Override
    public String toString() {
        return "SprocPerfValue{" +
                "schema='" + schema + '\'' +
                ", name='" + name + '\'' +
                ", parameters='" + parameters + '\'' +
                ", parameterModes='" + parameterModes + '\'' +
                ", collisions=" + collisions +
                ", totalCalls=" + totalCalls +
                ", selfTime=" + selfTime +
                ", totalTime=" + totalTime +
                '}';
    }
}
