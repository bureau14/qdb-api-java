package com.b14.qdb.hadoop.mapreduce.mahout;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.b14.qdb.hadoop.mahout.QuasardbPreference;

/**
 * A unit test case for {@link QuasardbPreference} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class QuasardbPreferenceTest {

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link QuasardbPreference#QuasardbPreference(long, long, float, long)}.
     */
    @Test
    public void testQuasardbPreference() {
        QuasardbPreference pref = null;
        try {
            pref = new QuasardbPreference(123L, 124L, 0.5F, (new Date()).getTime());
            assertTrue(pref.getUserID() == 123L);
            assertTrue(pref.getItemID() == 124L);
            assertTrue(pref.getValue() == 0.5F);
        } catch (Exception e) {
            fail("Shouldn't raise an Exception");
        }
    }

    /**
     * Test method for {@link QuasardbPreference#QuasardbPreference(long, long, float, long)}.
     */
    @Test
    public void testQuasardbPreferenceWithNaNValueMeansAnException() {
        QuasardbPreference pref = null;
        try {
            pref = new QuasardbPreference(123L, 124L, Float.NaN, (new Date()).getTime());
            fail("Shouldn't raise an Exception");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(pref == null);
        }
    }
    
    /**
     * Test method for {@link QuasardbPreference#setValue(float)}.
     */
    @Test
    public void testSetValue() {
        QuasardbPreference pref = null;
        try {
            pref = new QuasardbPreference(123L, 124L, 0.5F, (new Date()).getTime());
            assertTrue(pref.getUserID() == 123L);
            assertTrue(pref.getItemID() == 124L);
            assertTrue(pref.getValue() == 0.5F);
            pref.setValue(0.1F);
            assertTrue(pref.getValue() == 0.1F);
        } catch (Exception e) {
            fail("Shouldn't raise an Exception");
        }
    }

    /**
     * Test method for {@link QuasardbPreference#setValue(float)}.
     */
    @Test
    public void testSetValueWithNaNMeansAnException() {
        QuasardbPreference pref = null;
        try {
            pref = new QuasardbPreference(123L, 124L, 0.5F, (new Date()).getTime());
            assertTrue(pref.getUserID() == 123L);
            assertTrue(pref.getItemID() == 124L);
            assertTrue(pref.getValue() == 0.5F);
            
            pref.setValue(Float.NaN);
            fail("Shouldn't raise an Exception");
        } catch (Exception e) {
            assertTrue(pref.getValue() == 0.5F);
            assertTrue(e instanceof IllegalArgumentException);
        }
    }
    
    /**
     * Test method for {@link QuasardbPreference#equals(Object)}.
     */
    @Test
    public void testEquals() {
        QuasardbPreference qdbPref1 = new QuasardbPreference(123L, 124L, 0.5F, (new Date()).getTime());
        assertTrue(!qdbPref1.equals(null));
        assertTrue(qdbPref1.equals(qdbPref1));
        assertTrue(!qdbPref1.equals(new String("test")));
        
        QuasardbPreference qdbPref2 = new QuasardbPreference(123L, 124L, 0.5F, (new Date()).getTime());
        assertTrue(qdbPref1.equals(qdbPref2));
        
        QuasardbPreference qdbPref3 = new QuasardbPreference(1123L, 124L, 0.5F, (new Date()).getTime());
        assertTrue(!qdbPref1.equals(qdbPref3));
        
        qdbPref3 = new QuasardbPreference(123L, 1124L, 0.5F, (new Date()).getTime());
        assertTrue(!qdbPref1.equals(qdbPref3));
        
        qdbPref3 = new QuasardbPreference(123L, 124L, 1.0F, (new Date()).getTime());
        assertTrue(!qdbPref1.equals(qdbPref3));
        
        qdbPref3 = new QuasardbPreference(123L, 124L, 0.5F, (new Date(1427291844L)).getTime());
        assertTrue(!qdbPref1.equals(qdbPref3));
    }
    
    /**
     * Test method for {@link QuasardbPreference#equals(Object)}.
     */
    @Test
    public void testHashCode() {
        QuasardbPreference qdbPref1 = new QuasardbPreference(123L, 124L, 0.5F, (new Date()).getTime());
        QuasardbPreference qdbPref2 = new QuasardbPreference(123L, 124L, 0.5F, (new Date()).getTime());
        assertTrue(qdbPref1.hashCode() == qdbPref2.hashCode());
    }
}
