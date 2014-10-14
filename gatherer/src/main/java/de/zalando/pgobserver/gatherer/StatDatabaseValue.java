package de.zalando.pgobserver.gatherer;

import java.sql.Timestamp;


public class StatDatabaseValue {
    Timestamp timestamp;
    int numbackends;
    long xact_commit;
    long xact_rollback;
    long blks_read;
    long blks_hit;
    long temp_files;
    long temp_bytes;
    long deadlocks;
    long blk_read_time;
    long blk_write_time;

    @Override
    public String toString() {
        return "StatDatabaseValue{" +
                "timestamp='" + timestamp + '\'' +
                "deadlocks=" + deadlocks +
                '}';
    }    
}
