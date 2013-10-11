/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.zalando.pgobserver.gatherer;

import junit.framework.TestCase;

/**
 *
 * @author kmoppel
 */
public class LoadGathererTest extends TestCase {
    
    public LoadGathererTest(String testName) {
        super(testName);
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test of xLogLocationToMb method, of class LoadGatherer.
     */
    public void testXLogLocationToMb1() {
        assertEquals(3087563L, LoadGatherer.xLogLocationToMb("2F1/CDABE000"));
    }
    
    /**
     * Test of xLogLocationToMb method, of class LoadGatherer.
     */
    public void testXLogLocationToMb2() {
        assertEquals(6L, LoadGatherer.xLogLocationToMb("0/1644148"));
    }
}
