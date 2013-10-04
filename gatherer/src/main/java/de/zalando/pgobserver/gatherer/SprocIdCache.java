package de.zalando.pgobserver.gatherer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author  jmussler
 */
public class SprocIdCache {

    protected Host host;
    protected Map<String, Map<String, Integer>> cache = null;

    public SprocIdCache(final Host h) {
        host = h;
        cache = new TreeMap<String, Map<String, Integer>>();
    }

    public int getId(final Connection conn, SprocPerfValue v ) {

        Map<String, Integer> m = cache.get(schema);
        if (m != null) {
            Map<String, Integer> newM = new HashMap<String, Integer>();
            cache.put(schema,newM);
            m = newM;
        }

        String cacheKey = v.name+"|"+v.schema+"|"+v.paramters+"|"+v.paramterModes;
        Integer i = m.get(cacheKey);

        if (i == null) {
            i = newValue(conn, v);
            m.put(name, i);
        }

        return i;
    }

    private String getFunctionName(SprocPerfValue v) {
        List<String> types = Splitter.on(',').splitToList(v.paramters);
        List<String> modes = Splitter.on(',').splitToList(v.paramterModes);

        if(types.size()!=modes.size()) {
            LOG.error("List of Types and List of Modes is not equal in size");
            return v.name;
        }

        StringBuilder b = new StringBuilder();
        b.append(v.name).append("(")
        int i = 0;
        for(String t : types) {
            if(i>0) {
                b.append(',');
            }

            if(modes.get(i).equals("o")) {
                b.append(t);
            }
            else {
                b.append("i ").append(t);
            }
        }
        b.append(")");
        return b.build();
    }

    private String getWrongFunctionName(SprocPerfValue v) {
        List<String> types = Splitter.on(',').splitToList(v.paramters);
        List<String> modes = Splitter.on(',').splitToList(v.paramterModes);

        if(types.size()!=modes.size()) {
            LOG.error("List of Types and List of Modes is not equal in size");
            return v.name;
        }

        StringBuilder b = new StringBuilder();
        b.append(v.name).append("(")
        int i = 0;
        for(String t : types) {
            if(i>0) {
                b.append(',');
            }
            b.append(t);
        }
        b.append(")");

        return b.build();
    }

    private int getIdForName(final Connection conn, final String schema, final String name) throws SQLException {

        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("SELECT sproc_id FROM monitor_data.sprocs WHERE sproc_schema = '" + schema
                        + "' AND sproc_name = '" + name + "' AND sproc_host_id = " + host.id);
        if(rs.next()) {
            int id = rs.getInt("sproc_id");
            rs.close();
            s.close();
            return id;
        }
        return 0;
    }

    private int getCountForName(final Connection conn, final String schema, final String name) throws SQLException {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("SELECT count(1) AS count FROM monitor_data.sprocs WHERE sproc_schema = '" + schema
                        + "' AND sproc_name = '" + name + "' AND sproc_host_id = " + host.id);
        
        int count = rs.getInt("count");
        rs.close();
        s.close();
        return count;        
    }

    public int newValue(final Connection conn, SprocPerfValue v) throws SQLException {
        int id = getIdForName(conn,v.schema,getFunctionName(v));
        if(id==0) {
            int idOld = getIdForName(conn,v.schema,getWrongFunctionName(v));
            if(idOld!=0) {
                int count = getCountForName(conn,v.schema,getWrongFunctionName(v));
                if(count==1) {
                    // now we can update the signature, because it is unique
                }
            }
        }


        } else {
            rs.close();
            rs = s.executeQuery(
                    "INSERT INTO monitor_data.sprocs ( sproc_host_id, sproc_schema, sproc_name ) VALUES (" + host.id
                        + ", '" + schema + "','" + name + "' ) RETURNING sproc_id");
            if (rs.next()) {
                int id = rs.getInt("sproc_id");
                rs.close();
                s.close();
                return id;
            } else {
                s.close();
                return 0;
            }
        }
    }
}
