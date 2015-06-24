/**
 *
 */
package com.b14.qdb;

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
public class QdbQueueTest {
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
	 * Test method for {@link com.b14.qdb.QdbQueue#size()}.
	 * @throws QdbException
	 */
/*	@Test
	public void testSize() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueue");

        assertEquals(queue.size(), 0);
        assertTrue(queue.isEmpty());
	}*/

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#alias()}.
	 * @throws QdbException
	 */
	@Test
	public void testAlias() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueue");
		assertTrue("testQueue".equals(queue.getAlias()));
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#addFirst(java.nio.ByteBuffer)}.
	 * @throws QdbException
	 */
	@Test
	public void testAddFirst() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueAddFirst");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.addFirst(content);

   /*     assertEquals(queue.size(), 1);

        java.nio.ByteBuffer got = queue.get(0);
        assertEquals(content, got);*/

        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.addFirst(content);

    /*    assertEquals(queue.size(), 2);

        got = queue.get(1);
        assertEquals(content, got);*/

        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.addFirst(content);

   /*     assertEquals(queue.size(), 3);

        got = queue.get(2);
        assertEquals(content, got);

        assertTrue(!queue.isEmpty()); */

        java.nio.ByteBuffer buffer = queue.pollFirst();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#addLast(java.nio.ByteBuffer)}.
	 * @throws QdbException
	 */
	@Test
	public void testAddLast() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueAddLast");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.addLast(content);

        java.nio.ByteBuffer buffer = queue.pollLast();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#pollFirst()}.
	 * @throws QdbException
	 */
	@Test
	public void testPollFirst() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueuePollFirst");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.addLast(content);

        java.nio.ByteBuffer buffer = queue.pollFirst();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));

		buffer = queue.pollFirst();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA2.equals(new String(bytes)));

		buffer = queue.pollFirst();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#pollLast()}.
	 * @throws QdbException
	 */
	@Test
	public void testPollLast() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueuePollLast");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.addLast(content);

        java.nio.ByteBuffer buffer = queue.pollLast();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));

		buffer = queue.pollLast();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA2.equals(new String(bytes)));

		buffer = queue.pollLast();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#peekFirst()}.
	 * @throws QdbException
	 */
	@Test
	public void testPeekFirst() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueuePeekFirst");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.addLast(content);

        java.nio.ByteBuffer buffer = queue.peekFirst();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));

		buffer = queue.peekFirst();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));

	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#peekLast()}.
	 * @throws QdbException
	 */
	@Test
	public void testPeekLast() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueuePeekLast");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.addLast(content);

        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.addLast(content);

        java.nio.ByteBuffer buffer = queue.peekLast();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));

		buffer = queue.peekLast();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
	}

}
