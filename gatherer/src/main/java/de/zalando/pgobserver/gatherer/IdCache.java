package de.zalando.pgobserver.gatherer;

import java.sql.Connection;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author  jmussler
 */
public abstract class IdCache {
    protected Host host;
    protected Map<String, Map<String, Integer>> cache = null;

    public IdCache(final Host h) {
        host = h;
        cache = new TreeMap<String, Map<String, Integer>>();
    }

    public abstract int newValue(Connection conn, String schema, String name);

    public int getId(final Connection conn, final String schema, final String name) {
        Map<String, Integer> m = cache.get(schema);

        if (m != null) {
            Integer i = m.get(name);
            if (i == null) {
                i = newValue(conn, schema, name);
                m.put(name, i);
                return i;
            } else {
                return i;
            }
        } else {
            int i = newValue(conn, schema, name);

            Map<String, Integer> newM = new HashMap<String, Integer>();

            newM.put(name, i);
            cache.put(schema, newM);

            return i;
        }
    }
}
