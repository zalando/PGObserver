package de.zalando.pgobserver.gatherer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.logging.Level;
import java.util.logging.Logger;


public class IndexIdCache extends IdCache {
    public IndexIdCache(final Host h) {
        super(h);
    }

    @Override
    public int newValue(final Connection conn, final String schema, final String name) {

        try {
            String[] splits = name.split("::");
            String tableName = splits[0];
            String indexName = splits[1];
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("SELECT i_id FROM monitor_data.indexes WHERE i_schema = '" + schema
                        + "' AND i_name = '" + indexName + "' AND i_table_name = '" + tableName + "' AND i_host_id = " + host.id);
            if (rs.next()) {
                int id = rs.getInt("i_id");
                rs.close();
                s.close();
                return id;
            } else {
                rs.close();
                rs = s.executeQuery("INSERT INTO monitor_data.indexes ( i_host_id, i_schema, i_name, i_table_name ) VALUES (" + host.id
                            + ", '" + schema + "','" + indexName + "','" + tableName + "' ) RETURNING i_id");
                if (rs.next()) {
                    int id = rs.getInt("i_id");
                    rs.close();
                    s.close();
                    return id;
                } else {
                    s.close();
                    return 0;
                }
            }

        } catch (SQLException ex) {
            Logger.getLogger(IndexIdCache.class.getName()).log(Level.SEVERE, "", ex);
            return 0;
        } finally { }
    }

}
