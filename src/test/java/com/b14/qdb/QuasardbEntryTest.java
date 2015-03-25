package com.b14.qdb;

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

public class QuasardbEntryTest {   
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link QuasardbEntry#hashCode()}.
     */
    @Test
    public void testHashCode() {
        QuasardbEntry<String> entry1 = new QuasardbEntry<String>();
        entry1.setAlias("alias");
        entry1.setValue("value");
        QuasardbEntry<String> entry2 = new QuasardbEntry<String>();
        entry2.setAlias("alias");
        entry2.setValue("value");
        
        assertTrue(entry1.hashCode() == entry2.hashCode());
    }

    /**
     * Test method for {@link QuasardbEntry#QuasardbEntry()}.
     */
    @Test
    public void testQuasardbEntry() {
        QuasardbEntry<String> entry = new QuasardbEntry<String>();
        assertTrue(entry.getAlias() == null);
        assertTrue(entry.getValue() == null);
    }

    /**
     * Test method for {@link QuasardbEntry#QuasardbEntry(String, Object)}.
     */
    @Test
    public void testQuasardbEntryWithArgs() {
        QuasardbEntry<String> entry = new QuasardbEntry<String>("alias", "value");
        assertTrue(entry.getAlias() != null);
        assertTrue(entry.getAlias().equals("alias"));
        assertTrue(entry.getValue() != null);
        assertTrue(entry.getValue().equals("value"));
    }

    /**
     * Test method for {@link QuasardbEntry#setAlias(String)}.
     */
    @Test
    public void testGetAlias() {
        QuasardbEntry<String> entry = new QuasardbEntry<String>();
        assertTrue(entry.getAlias() == null);
        entry.setAlias("alias");
        assertTrue(entry.getAlias().equals("alias"));
    }

    /**
     * Test method for {@link QuasardbEntry#setAlias(String)}.
     */
    @Test
    public void testSetAlias() {
        QuasardbEntry<String> entry = new QuasardbEntry<String>();
        entry.setAlias("alias");
        assertTrue(entry.getAlias().equals("alias"));
    }

    /**
     * Test method for {@link QuasardbEntry#getValue()}.
     */
    @Test
    public void testGetValue() {
        QuasardbEntry<String> entry = new QuasardbEntry<String>();
        assertTrue(entry.getValue() == null);
        entry.setValue("value");
        assertTrue(entry.getValue().equals("value"));
    }

    /**
     * Test method for {@link QuasardbEntry#setValue(Object)}.
     */
    @Test
    public void testSetValue() {
        QuasardbEntry<String> entry = new QuasardbEntry<String>();
        entry.setValue("value");
        assertTrue(entry.getValue().equals("value"));
    }

    /**
     * Test method for {@link QuasardbEntry#equals(Object)}.
     */
    @Test
    public void testEqualsObject() {
        QuasardbEntry<String> entry1 = new QuasardbEntry<String>();
        entry1.setAlias("alias");
        entry1.setValue("value");
        QuasardbEntry<String> entry2 = new QuasardbEntry<String>();
        entry2.setAlias("alias");
        entry2.setValue("value");
    
        assertTrue(entry1.equals(entry2));
    }

    @Test
    public void testToString() {
        QuasardbEntry<String> entry = new QuasardbEntry<String>("alias", "value");
        assertTrue(entry.toString() != null);
        assertTrue(!entry.toString().isEmpty());
        String valuesString = entry.toString().substring(entry.toString().indexOf('[') + 1, entry.toString().length() - 1);
        String[] data = valuesString.split(",");
        final Properties p = new Properties();
        try {
            for (String prop : data) {
                p.load(new StringReader(prop));
            }
        } catch (IOException e) {
            fail("Shouldn't raise an exception");
        }
        assertTrue("alias".equals(p.get("alias")));
        assertTrue("value".equals(p.get("value")));
    }

}
