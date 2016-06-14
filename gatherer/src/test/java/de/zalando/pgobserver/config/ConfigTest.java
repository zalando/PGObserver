package de.zalando.pgobserver.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.Reader;
import java.io.StringReader;

import org.junit.Test;
import de.zalando.pgobserver.gatherer.config.Config;

public class ConfigTest {

	
	@Test
	public void configReadWithPoolDefaultFile(){
		Config config = Config.LoadConfigFromFile("pgobserver_gatherer.example.yaml");
		assertNotNull(config);
		assertNotNull(config.pool);
		Integer x = config.pool.partitions;
		assertEquals(x.intValue(), 2);
		Integer max = config.pool.maxConnectionsPerPartition;
		assertEquals(max.intValue(), 20);
	}
	
	@Test
	public void configReadPoolMissing(){
		// if there is no pool in config yml, defaults should apply
		String yaml = "database:\n  name: local_pgo_db\n  host: localhost";
		Reader reader = new StringReader(yaml);
		Config config = Config.LoadConfigFromStream(reader);
		assertNotNull(config);
		assertNotNull(config.pool);
		assertEquals(config.database.name, "local_pgo_db");
		Integer x = config.pool.partitions;
		assertEquals(x.intValue(), 2);
		Integer max = config.pool.maxConnectionsPerPartition;
		assertEquals(max.intValue(), 20);
		
	}
	
	@Test
	public void configReadWithPoolNonDefaultPartitions() {
		// if there are properties defined they should overrule
		String yaml = "database:\n  name: local_pgo_db\n  host: localhost\npool:\n  partitions: 3";
		Reader reader = new StringReader(yaml);
		Config config = Config.LoadConfigFromStream(reader);
		assertNotNull(config);
		assertNotNull(config.pool);
		Integer x = config.pool.partitions;
		assertEquals(x.intValue(), 3);
		Integer max = config.pool.maxConnectionsPerPartition;
		assertEquals(max.intValue(), 20);
	}
}
