package de.zalando.pgobserver.gatherer;


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

    @Override
    public String toString() {
        return "TableStatsValue{" +
                "schema='" + schema + '\'' +
                ", name='" + name + '\'' +
                ", table_size=" + table_size +
                ", index_size=" + index_size +
                ", seq_scans=" + seq_scans +
                ", index_scans=" + index_scans +
                ", tup_inserted=" + tup_inserted +
                ", tup_updated=" + tup_updated +
                ", tup_deleted=" + tup_deleted +
                ", tup_hot_updated=" + tup_hot_updated +
                '}';
    }
}
