/**
 *
 */
package net.quasardb.qdb;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class QdbDequeTest {
    private static final String DATA1 = "This is my data test 1";
    private static final String DATA2 = "This is my data test 2";
    private static final String DATA3 = "This is my data test 3";
    private QdbCluster cluster = null;

    @Before
    public void setUp() {
        try {
            cluster = new QdbCluster(DaemonRunner.getURI());
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
     * Test method for {@link net.quasardb.qdb.QdbDeque#size()}.
     * @throws QdbException
     */
    /*    @Test
    public void testSize() throws QdbException {
        QdbDeque deque = cluster.getDeque("testDeque");

        assertEquals(deque.size(), 0);
        assertTrue(deque.isEmpty());
    }*/

    /**
     * Test method for {@link net.quasardb.qdb.QdbDeque#alias()}.
     * @throws QdbException
     */
    @Test
    public void testAlias() throws QdbException {
        QdbDeque deque = cluster.getDeque("testDeque");
        assertTrue("testDeque".equals(deque.getAlias()));
    }

    /**
     * Test method for {@link net.quasardb.qdb.QdbDeque#addFirst(java.nio.ByteBuffer)}.
     * @throws QdbException
     */
    @Test
    public void testAddFirst() throws QdbException {
        QdbDeque deque = cluster.getDeque("testDequeAddFirst");
        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        deque.addFirst(content);

        /*     assertEquals(deque.size(), 1);

        java.nio.ByteBuffer got = deque.get(0);
        assertEquals(content, got);*/

        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        deque.addFirst(content);

        /*    assertEquals(deque.size(), 2);

        got = deque.get(1);
        assertEquals(content, got);*/

        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        deque.addFirst(content);

        /*     assertEquals(deque.size(), 3);

        got = deque.get(2);
        assertEquals(content, got);

        assertTrue(!deque.isEmpty()); */

        java.nio.ByteBuffer buffer = deque.pollFirst();
        byte[] bytes = new byte[buffer.limit()];
        buffer.rewind();
        buffer.get(bytes, 0, buffer.limit());
        assertTrue(DATA3.equals(new String(bytes)));
    }

    /**
     * Test method for {@link net.quasardb.qdb.QdbDeque#addLast(java.nio.ByteBuffer)}.
     * @throws QdbException
     */
    @Test
    public void testAddLast() throws QdbException {
        QdbDeque deque = cluster.getDeque("testDequeAddLast");
        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        deque.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        deque.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        deque.addLast(content);

        java.nio.ByteBuffer buffer = deque.pollLast();
        byte[] bytes = new byte[buffer.limit()];
        buffer.rewind();
        buffer.get(bytes, 0, buffer.limit());
        assertTrue(DATA3.equals(new String(bytes)));
    }

    /**
     * Test method for {@link net.quasardb.qdb.QdbDeque#pollFirst()}.
     * @throws QdbException
     */
    @Test
    public void testPollFirst() throws QdbException {
        QdbDeque deque = cluster.getDeque("testDequePollFirst");
        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        deque.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        deque.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        deque.addLast(content);

        java.nio.ByteBuffer buffer = deque.pollFirst();
        byte[] bytes = new byte[buffer.limit()];
        buffer.rewind();
        buffer.get(bytes, 0, buffer.limit());
        assertTrue(DATA1.equals(new String(bytes)));

        buffer = deque.pollFirst();
        bytes = new byte[buffer.limit()];
        buffer.rewind();
        buffer.get(bytes, 0, buffer.limit());
        assertTrue(DATA2.equals(new String(bytes)));

        buffer = deque.pollFirst();
        bytes = new byte[buffer.limit()];
        buffer.rewind();
        buffer.get(bytes, 0, buffer.limit());
        assertTrue(DATA3.equals(new String(bytes)));
    }

    /**
     * Test method for {@link net.quasardb.qdb.QdbDeque#pollLast()}.
     * @throws QdbException
     */
    @Test
    public void testPollLast() throws QdbException {
        QdbDeque deque = cluster.getDeque("testDequePollLast");
        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        deque.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        deque.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        deque.addLast(content);

        java.nio.ByteBuffer buffer = deque.pollLast();
        byte[] bytes = new byte[buffer.limit()];
        buffer.rewind();
        buffer.get(bytes, 0, buffer.limit());
        assertTrue(DATA3.equals(new String(bytes)));

        buffer = deque.pollLast();
        bytes = new byte[buffer.limit()];
        buffer.rewind();
        buffer.get(bytes, 0, buffer.limit());
        assertTrue(DATA2.equals(new String(bytes)));

        buffer = deque.pollLast();
        bytes = new byte[buffer.limit()];
        buffer.rewind();
        buffer.get(bytes, 0, buffer.limit());
        assertTrue(DATA1.equals(new String(bytes)));
    }

    /**
     * Test method for {@link net.quasardb.qdb.QdbDeque#peekFirst()}.
     * @throws QdbException
     */
    @Test
    public void testPeekFirst() throws QdbException {
        QdbDeque deque = cluster.getDeque("testDequePeekFirst");
        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        deque.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        deque.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        deque.addLast(content);

        java.nio.ByteBuffer buffer = deque.peekFirst();
        byte[] bytes = new byte[buffer.limit()];
        buffer.rewind();
        buffer.get(bytes, 0, buffer.limit());
        assertTrue(DATA1.equals(new String(bytes)));

        buffer = deque.peekFirst();
        bytes = new byte[buffer.limit()];
        buffer.rewind();
        buffer.get(bytes, 0, buffer.limit());
        assertTrue(DATA1.equals(new String(bytes)));
    }

    /**
     * Test method for {@link net.quasardb.qdb.QdbDeque#peekLast()}.
     * @throws QdbException
     */
    @Test
    public void testPeekLast() throws QdbException {
        QdbDeque deque = cluster.getDeque("testDequePeekLast");
        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        deque.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        deque.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        deque.addLast(content);

        java.nio.ByteBuffer buffer = deque.peekLast();
        byte[] bytes = new byte[buffer.limit()];
        buffer.rewind();
        buffer.get(bytes, 0, buffer.limit());
        assertTrue(DATA3.equals(new String(bytes)));

        buffer = deque.peekLast();
        bytes = new byte[buffer.limit()];
        buffer.rewind();
        buffer.get(bytes, 0, buffer.limit());
        assertTrue(DATA3.equals(new String(bytes)));
    }
}
