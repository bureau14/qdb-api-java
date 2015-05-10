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
public class QdbHashSetIT {
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
        assertTrue("testSet".equals(set.alias()));
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#size()}.
     * @throws QdbException 
     */
    @Test
    public void testSize() throws QdbException {
        QdbHashSet set = cluster.getSet("testSet");
        try {
            set.size();
            fail("Should raise an Exception");
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
        }
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#isEmpty()}.
     * @throws QdbException 
     */
    @Test
    public void testIsEmpty() throws QdbException {
        QdbHashSet set = cluster.getSet("testSet");
        try {
            set.isEmpty();
            fail("Should raise an Exception");
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
        }
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
        set.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        set.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        set.add(content);
        
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
     * Test method for {@link com.b14.qdb.QdbHashSet#iterator()}.
     * @throws QdbException 
     */
    @Test
    public void testIterator() throws QdbException {
        QdbHashSet set = cluster.getSet("testSet");
        try {
            set.iterator();
            fail("Should raise an Exception");
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
        }
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#toArray()}.
     * @throws QdbException 
     */
    @Test
    public void testToArray() throws QdbException {
        QdbHashSet set = cluster.getSet("testSet");
        try {
            set.toArray();
            fail("Should raise an Exception");
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
        }
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#toArray(T[])}.
     * @throws QdbException 
     */
    @Test
    public void testToArrayTArray() throws QdbException {
        QdbHashSet set = cluster.getSet("testSet");
        try {
            set.toArray(null);
            fail("Should raise an Exception");
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
        }
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#add(java.nio.ByteBuffer)}.
     * @throws QdbException 
     */
    @Test
    public void testAdd() throws QdbException {
        QdbHashSet set = cluster.getSet("testSetAdd");
        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        set.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        set.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        set.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        assertTrue(set.contains(content));
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#remove(java.lang.Object)}.
     * @throws QdbException 
     */
    @Test
    public void testRemove() throws QdbException {
        QdbHashSet set = cluster.getSet("testSetRemove");
        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        set.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        set.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        set.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        assertTrue(set.contains(content));
        
        set.remove(content);
        assertTrue(!set.contains(content));
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        assertTrue(set.contains(content));
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#containsAll(java.util.Collection)}.
     * @throws QdbException 
     */
    @Test
    public void testContainsAll() throws QdbException {
        QdbHashSet set = cluster.getSet("testSetAddAll");
        List<java.nio.ByteBuffer> list = new ArrayList<java.nio.ByteBuffer>();
        java.nio.ByteBuffer content1 = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content1.put(DATA1.getBytes());
        content1.flip();
        java.nio.ByteBuffer content2 = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content2.put(DATA2.getBytes());
        content2.flip();
        java.nio.ByteBuffer content3 = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content3.put(DATA3.getBytes());
        content3.flip();
        list.add(content1);
        list.add(content2);
        list.add(content3);
        set.addAll(list);
        
        assertTrue(set.containsAll(list));
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#addAll(java.util.Collection)}.
     * @throws QdbException 
     */
    @Test
    public void testAddAll() throws QdbException {
        QdbHashSet set = cluster.getSet("testSetAddAll");
        List<java.nio.ByteBuffer> list = new ArrayList<java.nio.ByteBuffer>();
        java.nio.ByteBuffer content1 = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content1.put(DATA1.getBytes());
        content1.flip();
        java.nio.ByteBuffer content2 = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content2.put(DATA2.getBytes());
        content2.flip();
        java.nio.ByteBuffer content3 = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content3.put(DATA3.getBytes());
        content3.flip();
        list.add(content1);
        list.add(content2);
        list.add(content3);
        set.addAll(list);
        
        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
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
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#retainAll(java.util.Collection)}.
     * @throws QdbException 
     */
    @Test
    public void testRetainAll() throws QdbException {
        QdbHashSet set = cluster.getSet("testRetainAll");
        try {
            set.retainAll(Collections.EMPTY_LIST);
            fail("Should raise an Exception");
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
        }
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#removeAll(java.util.Collection)}.
     * @throws QdbException 
     */
    @Test
    public void testRemoveAll() throws QdbException {
        QdbHashSet set = cluster.getSet("testSetRemoveAll");
        List<java.nio.ByteBuffer> list = new ArrayList<java.nio.ByteBuffer>();
        java.nio.ByteBuffer content1 = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content1.put(DATA1.getBytes());
        content1.flip();
        java.nio.ByteBuffer content2 = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content2.put(DATA2.getBytes());
        content2.flip();
        java.nio.ByteBuffer content3 = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content3.put(DATA3.getBytes());
        content3.flip();
        list.add(content1);
        list.add(content2);
        list.add(content3);
        set.addAll(list);
        
        assertTrue(set.containsAll(list));
        assertTrue(set.removeAll(list));
        assertTrue(!set.containsAll(list));
    }

    /**
     * Test method for {@link com.b14.qdb.QdbHashSet#clear()}.
     * @throws QdbException 
     */
    @Test
    public void testClear() throws QdbException {
        QdbHashSet set = cluster.getSet("testSetClear");
        List<java.nio.ByteBuffer> list = new ArrayList<java.nio.ByteBuffer>();
        java.nio.ByteBuffer content1 = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content1.put(DATA1.getBytes());
        content1.flip();
        java.nio.ByteBuffer content2 = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content2.put(DATA2.getBytes());
        content2.flip();
        java.nio.ByteBuffer content3 = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content3.put(DATA3.getBytes());
        content3.flip();
        list.add(content1);
        list.add(content2);
        list.add(content3);
        set.addAll(list);
        
        assertTrue(set.containsAll(list));
        set.clear();
        assertTrue(!set.containsAll(list));
    }

}
