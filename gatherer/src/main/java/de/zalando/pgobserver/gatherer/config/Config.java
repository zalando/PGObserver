package de.zalando.pgobserver.gatherer.config;

import de.zalando.pgobserver.gatherer.Host;
import de.zalando.pgobserver.gatherer.HostSettings;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author jmussler
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Config {
    public Database database;
    public Logfiles logfiles;
    public Frontend frontend;
    public String default_schema_filter;

    @Override
    public String toString() {
        return "Config{" + "database=" + database + ", logfiles=" + logfiles + ", frontend=" + frontend + '}';
    }
    
    public static Config LoadConfigFromFile(String s) {
        
        ObjectMapper mapper = new ObjectMapper();
        
        try {
            Config config = mapper.readValue(
                new File(s),
                Config.class);
            return config;
        } catch (IOException ex) {
            Logger.getLogger(Config.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return null;
        
    }
}
