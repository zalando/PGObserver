package de.zalando.pgobserver.gatherer;

public class IndexStatsValue {

    String schema;
    String indexrelname;
    String relname;
    long scan;
    long tup_read;
    long tup_fetch;
    long size;

    public boolean isEqualTo(final IndexStatsValue b) {
        return b.scan == scan && b.size == size && b.indexrelname == indexrelname
                && b.tup_fetch == tup_fetch && b.tup_read == tup_read && b.schema.equals(schema)
                && b.relname.equals(relname);
    }

    @Override
    public String toString() {
        return "IndexStatsValue{" +
                "schema='" + schema + '\'' +
                ", relname='" + relname + '\'' +
                ", indexrelname='" + indexrelname + '\'' +
                ", scans=" + scan +
                ", tup_read=" + tup_read +
                ", tup_fetch=" + tup_fetch +
                ", size=" + size +
                '}';
    }
}
