package de.zalando.pgobserver.gatherer.config;

import java.io.File;
import java.io.IOException;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Config {
    public Database database;
    public Frontend frontend;

    private static Logger LOG = LoggerFactory.getLogger(Config.class);

    @Override
    public String toString() {
        return "Config{" + "database=" + database + ", frontend=" + frontend + "}";
    }

    public static Config LoadConfigFromFile(ObjectMapper mapper, String s) {
        LOG.info("Reading config file from: {}", s);
        try {
            return mapper.readValue(
                new File(s),
                Config.class);
        } catch (IOException ex) {
            LOG.error("Error reading config file", ex);
        }
        return null;
    }
}
