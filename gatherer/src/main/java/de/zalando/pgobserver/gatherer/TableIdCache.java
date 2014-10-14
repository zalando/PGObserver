package de.zalando.pgobserver.gatherer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;


public class TableIdCache extends IdCache {
    public TableIdCache(final Host h) {
        super(h);
    }

    @Override
    public int newValue(final Connection conn, final String schema, final String name) {

        try {

            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("SELECT t_id FROM monitor_data.tables WHERE t_schema = '" + schema
                        + "' AND t_name = '" + name + "' AND t_host_id = " + host.id);
            if (rs.next()) {
                int id = rs.getInt("t_id");
                rs.close();
                s.close();
                return id;
            } else {
                rs.close();
                rs = s.executeQuery("INSERT INTO monitor_data.tables ( t_host_id, t_schema, t_name ) VALUES (" + host.id
                            + ", '" + schema + "','" + name + "' ) RETURNING t_id");
                if (rs.next()) {
                    int id = rs.getInt("t_id");
                    rs.close();
                    s.close();
                    return id;
                } else {
                    s.close();
                    return 0;
                }
            }

        } catch (SQLException ex) {
            Logger.getLogger(TableIdCache.class.getName()).log(Level.SEVERE, "", ex);
            return 0;
        } finally { }
    }

}
