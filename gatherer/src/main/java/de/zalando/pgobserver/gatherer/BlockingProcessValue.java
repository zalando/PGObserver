package de.zalando.pgobserver.gatherer;

import java.sql.Timestamp;


public class BlockingProcessValue {
    int bp_host_id;
    Timestamp bp_timestamp;
    int datid;
    String datname;
    int pid;
    int usesysid;
    String usename;
    String application_name;
    String client_addr;
    String client_hostname;
    int client_port;
    Timestamp backend_start;
    Timestamp xact_start;
    Timestamp query_start;
    Timestamp state_change;
    boolean waiting;
    String state;
    String query;    

    @Override
    public String toString() {
        return String.format("BlockingProcessValue{%s,%s,%s,%s,%s}",
            bp_host_id,
            bp_timestamp,
            datname,
            query,
            waiting
        );
    }
}    
