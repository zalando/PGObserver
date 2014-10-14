package de.zalando.pgobserver.gatherer;

import java.sql.Timestamp;


public class BlockingLockValue {
    int bl_host_id;
    Timestamp bl_timestamp;
    String locktype;
    int database;
    int relation;
    int page;
    short tuple;
    String virtualxid;
    String transactionid;
    int classid;
    int objid;
    short objsubid;
    String virtualtransaction;
    int pid;
    String mode;
    boolean granted;
    boolean fastpath;
    
    @Override
    public String toString() {
        return String.format("BlockingLockValue{%s,%s,%s}",
            bl_host_id,
            bl_timestamp,
            pid
        );
    }
}    
