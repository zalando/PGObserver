package de.zalando.pgobserver.gatherer;

import java.sql.Timestamp;


public class SchemaStatsValue {
    Timestamp timestamp;
    int hostId;
    String schemaName;
    long sprocCalls;
    long seqScans;
    long idxScans;
    long tupIns;
    long tupUpd;
    long tupDel;

    @Override
    public String toString() {
        return String.format("SchemaStatsValue{%s,%s,%s,%s,%s,%s,%s,%s,%s}",
            timestamp,
            hostId,
            schemaName,
            sprocCalls,
            seqScans,
            idxScans,
            tupIns,
            tupUpd,
            tupDel
        );
    }
}    
