package de.zalando.pgobserver.gatherer;

/**
 * @author  jmussler
 */
public class TableStatsValue {

    String schema;
    String name;
    long table_size;
    long index_size;
    long seq_scans;
    long index_scans;
    long tup_inserted;
    long tup_updated;
    long tup_deleted;
    long tup_hot_updated;

    public boolean isEqualTo(final TableStatsValue b) {
        return b.index_scans == index_scans && b.index_size == index_size && b.table_size == table_size
                && b.seq_scans == seq_scans && b.tup_deleted == tup_deleted && b.tup_hot_updated == tup_hot_updated
                && b.tup_inserted == tup_inserted && b.tup_updated == tup_updated && b.schema.equals(schema)
                && b.name.equals(name);
    }
}
