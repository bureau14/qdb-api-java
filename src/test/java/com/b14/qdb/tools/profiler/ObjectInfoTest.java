package com.b14.qdb.tools.profiler;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A unit test case for {@link ObjectrInfo} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.2.1
 */
public class ObjectInfoTest {
    private static final String NAME = "name";
    private static final String TYPE = "java.lang.String";
    private static final String VALUE = "ma valeur";
    private static final int offset = 128;
    private static final int length = 512;
    private static final int arraySize = 1024;
    private static final int arrayBase = 2048;
    private static final int arrayElementSize = 1;
    
    private final ObjectInfo objInfo = new ObjectInfo(NAME, TYPE, VALUE, offset, length, arraySize, arrayBase, arrayElementSize);
    
    @BeforeClass
    public static void setUpClass() throws Exception {
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }
    
    @Test
    public void testToString() {
        String stringToAnalyze = objInfo.toString();
        assertTrue("ToString shouldn't be null", stringToAnalyze != null);
        assertTrue("ToString shouldn't be empty", !stringToAnalyze.isEmpty());
        String[] data = stringToAnalyze.split(",");        
        final Properties p = new Properties();
        try {
            for (String prop : data) {
                p.load(new StringReader(prop));
            }
        } catch (IOException e) {
            fail("Shouldn't raise an exception");
        }
        assertTrue(p.get("name").equals(NAME));
        assertTrue(p.get("type").equals(TYPE));
        assertTrue(p.get("contents").equals(VALUE));
        assertTrue(p.get("offset").equals("" + offset));
        assertTrue(p.get("length").equals("" + length));
        assertTrue(p.get("arraySize").equals("" + arraySize));
        assertTrue(p.get("arrayBase").equals("" + arrayBase));
        assertTrue(p.get("arrayElemSize").equals("" + arrayElementSize));
    }
    
    @Test
    public void testGetDeepSizeWithNoChild() {
        assertTrue(objInfo.getDeepSize() == (length + arraySize));
    }
    
    @Test
    public void testGetDeepSizeWithChild() {
        objInfo.addChild(new ObjectInfo(NAME, TYPE, VALUE, offset, length, arraySize, arrayBase, arrayElementSize));
        assertTrue(objInfo.getDeepSize() > (length + arraySize));
    }
    
    @Test
    public void testAddChild() {
        objInfo.addChild(null);
        assertTrue(objInfo.getDeepSize() == (length + arraySize));
    }
}
