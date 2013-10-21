package de.zalando.pgobserver.gatherer;

import com.google.common.base.Splitter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jmussler
 */
public class SprocIdCache {

    private static final Logger LOG = LoggerFactory.getLogger(SprocIdCache.class);
    protected Host host;
    protected Map<String, Map<String, Integer>> cache = null;

    public SprocIdCache(final Host h) {
        host = h;
        cache = new TreeMap<String, Map<String, Integer>>();
    }

    public int getId(final Connection conn, SprocPerfValue v) throws SQLException {

        Map<String, Integer> m = cache.get(v.schema);
        if (m == null) {
            Map<String, Integer> newM = new HashMap<String, Integer>();
            cache.put(v.schema, newM);
            m = newM;
        }

        String cacheKey = getFunctionName(v);
        Integer i = m.get(cacheKey);
        if (i == null) {
            i = newValue(conn, v);
            m.put(cacheKey, i);
        }
        return i;
    }

    public static String getFunctionName(SprocPerfValue v) {
        List<String> types;
        List<String> modes;

        if (v.parameters == null || "".equals(v.parameters)
                || v.parameterModes == null || "".equals(v.parameterModes)) {
            types = new ArrayList<>(0);
            modes = new ArrayList<>(0);
        } else {
            types = Splitter.on(',').splitToList(v.parameters);
            modes = Splitter.on(',').splitToList(v.parameterModes);
        }

        if (types.size() != modes.size()) {
            LOG.error("List of Types and List of Modes is not equal in size");
            return v.name;
        }

        StringBuilder b = new StringBuilder();
        b.append(v.name).append("(");
        int i = 0;
        for (String t : types) {
            if (i > 0) {
                b.append(", ");
            }

            if (modes.get(i).equals("o")) {
                b.append(t);
            } else {
                b.append("i ").append(t);
            }
            i++;
        }
        b.append(")");
        return b.toString();
    }

    /**
     * generate the old function name, no distinction for in/out parameters
     *
     * @param v
     * @return
     */
    public static String getOldFunctionName(SprocPerfValue v) {
        List<String> types;
        List<String> modes;

        if (v.parameters == null || "".equals(v.parameters)
                || v.parameterModes == null || "".equals(v.parameterModes)) {
            types = new ArrayList<>(0);
            modes = new ArrayList<>(0);
        } else {
            types = Splitter.on(',').splitToList(v.parameters);
            modes = Splitter.on(',').splitToList(v.parameterModes);
        }

        if (types.size() != modes.size()) {
            LOG.error("List of Types and List of Modes is not equal in size");
            return v.name;
        }

        if (types.size() != modes.size()) {
            LOG.error("List of Types and List of Modes is not equal in size");
            return v.name;
        }

        StringBuilder b = new StringBuilder();
        b.append(v.name).append("(");
        int i = 0;
        for (String t : types) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(t);
            i++;
        }
        b.append(")");

        return b.toString();
    }

    private int getIdForName(final Connection conn, final String schema, final String name) throws SQLException {

        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("SELECT sproc_id FROM monitor_data.sprocs "
                + "WHERE sproc_schema = '" + schema + "'"
                + "AND sproc_name = '" + name + "'"
                + "AND sproc_host_id = " + host.id);
        if (rs.next()) {
            int id = rs.getInt("sproc_id");
            rs.close();
            s.close();
            return id;
        }
        return 0;
    }

    private int getCountForName(final Connection conn, final String schema, final String name) throws SQLException {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("SELECT count(1) AS count "
                + "FROM monitor_data.sprocs "
                + "WHERE sproc_schema = '" + schema + "'"
                + "AND sproc_name = '" + name + "'"
                + "AND sproc_host_id = " + host.id);

        int count = 0;
        if (rs.next()) {
            count = rs.getInt("count");
        }
        rs.close();
        s.close();
        return count;

    }

    /**
     * Try to rewrite all function names to proper new names, also accross
     * multiple schemas ignoring API Version
     *
     * @param conn
     * @param oldName
     * @param name
     * @param schema
     * @throws SQLException
     */
    private void renameFunction(final Connection conn, final String oldName, final String name, final String schema) throws SQLException {
        Statement s = conn.createStatement();
        s.execute("UPDATE monitor_data.sprocs "
                + "SET sproc_name = '" + name + "' "
                + "WHERE sproc_name = '" + oldName + "' "
                + "AND (substring(sproc_schema from '(.*)_r') = substring('" + schema + "' from '(.*)_r') OR sproc_schema = '" + schema + "') "
                + "AND sproc_host_id = " + host.id);
        s.close();
    }

    private int createNewFunctionEntry(final Connection conn, final String schema, final String name) throws SQLException {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery(
                "INSERT INTO monitor_data.sprocs ( sproc_host_id, sproc_schema, sproc_name ) "
                + "VALUES (" + host.id + ", '" + schema + "','" + name + "') RETURNING sproc_id");

        if (rs.next()) {
            int id = rs.getInt(1);
            rs.close();
            s.close();
            return id;
        }

        return 0;
    }

    /**
     * If no function is found, either - create a new entry - rename old entry
     * to new name if function name is unique ( then it is probably the same
     * function )
     *
     * @param conn
     * @param v
     * @return
     * @throws SQLException
     */
    public int newValue(final Connection conn, SprocPerfValue v) throws SQLException {
        String name = getFunctionName(v);
        int id = getIdForName(conn, v.schema, name);

        if (id == 0) {
            String oldName = getOldFunctionName(v);

            int idOld = getIdForName(conn, v.schema, oldName);
            if (idOld != 0 && v.collisions == 1) {
                int count = getCountForName(conn, v.schema, oldName);
                if (count == 1) {
                    // update function name to new name, because no collision and realy only one function in pgobserver                    
                    renameFunction(conn, oldName, name, v.schema);
                    return idOld;
                }
            }

            /**
             * There is more than one matching or a collision on the observed
             * schema, so create a new function here
             */
            id = createNewFunctionEntry(conn, v.schema, name);
        }
        return id;
    }
}