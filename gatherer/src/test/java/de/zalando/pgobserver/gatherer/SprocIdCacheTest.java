package de.zalando.pgobserver.gatherer;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author jmussler
 */
public class SprocIdCacheTest {

    @Test
    public void testGetFunctionName() {
        System.out.println("getFunctionName");
        SprocPerfValue v = new SprocPerfValue();
        v.name = "some_function";
        v.parameters = "integer,integer,character varying";
        v.parameterModes = "i,o,o";
        String expResult = "some_function(i integer, integer, character varying)";
        String result = SprocIdCache.getFunctionName(v);
        assertEquals(expResult, result);
    }

    @Test
    public void testGetOldFunctionName() {
        System.out.println("getFunctionName");
        SprocPerfValue v = new SprocPerfValue();
        v.name = "some_function";
        v.parameters = "integer,integer,character varying";
        v.parameterModes = "i,o,o";
        String expResult = "some_function(integer, integer, character varying)";
        String result = SprocIdCache.getOldFunctionName(v);
        assertEquals(expResult, result);
    }
}
