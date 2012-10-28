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
public class SprocIdCache extends IdCache {

    // cache by schema and sproc name, return only id

    public SprocIdCache(final Host h) {
        super(h);
    }

    @Override
    public int newValue(final Connection conn, final String schema, final String name) {
        try {

            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("SELECT sproc_id FROM monitor_data.sprocs WHERE sproc_schema = '" + schema
                        + "' AND sproc_name = '" + name + "' AND sproc_host_id = " + host.id);
            if (rs.next()) {
                int id = rs.getInt("sproc_id");
                rs.close();
                s.close();
                return id;
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

        } catch (SQLException ex) {
            Logger.getLogger(SprocIdCache.class.getName()).log(Level.SEVERE, "", ex);
            return 0;
        } finally { }
    }
}
