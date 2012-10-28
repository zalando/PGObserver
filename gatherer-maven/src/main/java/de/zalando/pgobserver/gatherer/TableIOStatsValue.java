package de.zalando.pgobserver.gatherer;

/**
 * @author  jmussler
 */
public class TableIOStatsValue {
    String name;
    String schema;
    long heap_blk_read;
    long heap_blk_hit;
    long index_blk_read;
    long index_blk_hit;

    public boolean isEqualTo(final TableIOStatsValue b) {
        return heap_blk_hit == b.heap_blk_hit && heap_blk_read == b.heap_blk_read && index_blk_hit == b.index_blk_hit
                && index_blk_read == b.index_blk_read && name.equals(b.name) && schema.equals(b.schema);
    }
}
