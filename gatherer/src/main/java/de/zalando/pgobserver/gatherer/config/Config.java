package de.zalando.pgobserver.gatherer.config;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


@JsonIgnoreProperties(ignoreUnknown = true)
public class Config {
    public Database database;
    public Frontend frontend;

    @Override
    public String toString() {
        return "Config{" + "database=" + database + ", frontend=" + frontend + '}';
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
