/**
 * 
 */
package com.b14.qdb;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class QdbHashSetTest {
    private static final String URI = "qdb://127.0.0.1:2836";
    private static final String DATA1 = "This is my data test 1";
    private static final String DATA2 = "This is my data test 2";
    private static final String DATA3 = "This is my data test 3";
    
    private QdbCluster cluster = null;
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        Qdb.DAEMON.start();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        Qdb.DAEMON.stop();
    }

    @Before
    public void setUp() {
        try {
            cluster = new QdbCluster(URI);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        try {
            cluster.purgeAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#alias()}.
     * @throws QdbException 
     */
    @Test
    public void testAlias() throws QdbException {
        QdbHashSet set = cluster.getSet("testSet");
        assertTrue("testSet".equals(set.getAlias()));
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#contains(java.lang.Object)}.
     * @throws QdbException 
     */
    @Test
    public void testContains() throws QdbException {
        QdbHashSet set = cluster.getSet("testSetContains");
        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        assertTrue(set.insert(content));
        assertFalse(set.insert(content));
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        assertTrue(set.insert(content));
        assertFalse(set.insert(content));
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        assertTrue(set.insert(content));
        assertFalse(set.insert(content));
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        assertTrue(set.contains(content));
        content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        assertTrue(set.contains(content));
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        assertTrue(set.contains(content));
        content = java.nio.ByteBuffer.allocateDirect("test".getBytes().length);
        content.put("test".getBytes());
        content.flip();
        assertTrue(!set.contains(content));
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#insert(java.nio.ByteBuffer)}.
     * @throws QdbException 
     */
    @Test
    public void testInsert() throws QdbException {
        QdbHashSet set = cluster.getSet("testSetAdd");
        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        assertTrue(set.insert(content));
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        assertTrue(set.insert(content));
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        assertTrue(set.insert(content));
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        assertTrue(set.contains(content));
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#erase(java.lang.Object)}.
     * @throws QdbException 
     */
    @Test
    public void testErase() throws QdbException {
        QdbHashSet set = cluster.getSet("testSetRemove");
        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        assertTrue(set.insert(content));
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        assertTrue(set.insert(content));
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        assertTrue(set.insert(content));
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        assertTrue(set.contains(content));
        
        assertTrue(set.erase(content));
        assertFalse(set.erase(content));
        assertTrue(!set.contains(content));
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        assertTrue(set.contains(content));
    }

}
