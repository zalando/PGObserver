package de.zalando.pgobserver.gatherer.config;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Config {
    public Database database;
    public Frontend frontend;
    public Pool pool = new Pool();

    private static Logger LOG = LoggerFactory.getLogger(Config.class);

    @Override
    public String toString() {
        return "Config{" + "database=" + database + ", frontend=" + frontend + ", pool=" + pool +"}";
    }

    public static Config LoadConfigFromFile( final String file) {
        LOG.info("Reading config file from: {}", file);
        try {
            final Reader reader = new FileReader(file);
            return LoadConfigFromStream(reader);
        } catch (IOException ex) { // TODO: check FileNotFoundException
            LOG.error("Error reading config file", ex);
        }
        return null;
    }

	public static Config LoadConfigFromStream(Reader reader) {
        try {
            final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            return mapper.readValue(
                reader,
                Config.class);
        } catch (IOException ex) {
            LOG.error("Error reading configuration: ", ex);
        }
        return null;
	}
}
