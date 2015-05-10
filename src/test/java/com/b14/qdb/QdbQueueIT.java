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
public class QdbQueueIT {
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
	@Test
	public void testSize() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueue");
		try {
			queue.size();
		} catch (Exception e) {
			assertTrue(e instanceof UnsupportedOperationException);
		}
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#alias()}.
	 * @throws QdbException 
	 */
	@Test
	public void testAlias() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueue");
		assertTrue("testQueue".equals(queue.alias()));
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
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.addFirst(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.addFirst(content);
        
        assertTrue(!queue.isEmpty());
        
        java.nio.ByteBuffer buffer = queue.getFirst();
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
        
        assertTrue(!queue.isEmpty());
        
        java.nio.ByteBuffer buffer = queue.getLast();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#offerFirst(java.nio.ByteBuffer)}.
	 * @throws QdbException 
	 */
	@Test
	public void testOfferFirst() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueOfferFirst");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        assertTrue(queue.offerFirst(content));
        
        assertTrue(!queue.isEmpty());
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        assertTrue(queue.offerFirst(content));
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        assertTrue(queue.offerFirst(content));
        
        java.nio.ByteBuffer buffer = queue.getFirst();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#offerLast(java.nio.ByteBuffer)}.
	 * @throws QdbException 
	 */
	@Test
	public void testOfferLast() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueOfferLast");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        assertTrue(queue.offerLast(content));
        
        assertTrue(!queue.isEmpty());
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        assertTrue(queue.offerLast(content));
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        assertTrue(queue.offerLast(content));
        
        java.nio.ByteBuffer buffer = queue.getLast();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#removeFirst()}.
	 * @throws QdbException 
	 */
	@Test
	public void testRemoveFirst() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueRemoveFirst");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
        
        java.nio.ByteBuffer buffer = queue.removeFirst();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));
		
		buffer = queue.removeFirst();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA2.equals(new String(bytes)));
		
		buffer = queue.removeFirst();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
		
		try {
			queue.removeFirst();
			fail("Should raise an Exception");
		} catch (Exception e) {
			assertTrue(e instanceof NoSuchElementException);
			assertTrue(queue.isEmpty());
		}
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#removeLast()}.
	 * @throws QdbException 
	 */
	@Test
	public void testRemoveLast() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueRemoveLast");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
        
        java.nio.ByteBuffer buffer = queue.removeLast();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
		
		buffer = queue.removeLast();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA2.equals(new String(bytes)));
		
		buffer = queue.removeLast();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));
		
		try {
			queue.removeLast();
			fail("Should raise an Exception");
		} catch (Exception e) {
			assertTrue(e instanceof NoSuchElementException);
			assertTrue(queue.isEmpty());
		}
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
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
        
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
		
		try {
			assertTrue(queue.pollFirst() == null);
			assertTrue(queue.isEmpty());
		} catch (Exception e) {
			fail("Should raise an Exception");
		}
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
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
        
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
		
		try {
			assertTrue(queue.pollLast() == null);
			assertTrue(queue.isEmpty());
		} catch (Exception e) {
			fail("Should raise an Exception");
		}
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#getFirst()}.
	 * @throws QdbException 
	 */
	@Test
	public void testGetFirst() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueGetFirst");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
        
        java.nio.ByteBuffer buffer = queue.getFirst();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));
		
		buffer = queue.getFirst();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#getLast()}.
	 * @throws QdbException 
	 */
	@Test
	public void testGetLast() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueGetLast");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
        
        java.nio.ByteBuffer buffer = queue.getLast();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
		
		buffer = queue.getLast();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
		
		queue.poll();
		queue.poll();
		queue.poll();
		try {
			buffer = queue.getLast();
			fail("Should raise an Exception.");
		} catch (Exception e) {
			assertTrue(e instanceof NoSuchElementException);
			assertTrue(queue.isEmpty());
		}
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
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
        
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
		
		queue.poll();
		queue.poll();
		queue.poll();
		buffer = queue.peekFirst();
		assertTrue(buffer == null);
		assertTrue(queue.isEmpty());
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
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
        
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
		
		queue.poll();
		queue.poll();
		queue.poll();
		buffer = queue.peekLast();
		assertTrue(buffer == null);
		assertTrue(queue.isEmpty());
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#removeFirstOccurrence(java.lang.Object)}.
	 * @throws QdbException 
	 */
	@Test
	public void testRemoveFirstOccurrence() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueue");
		try {
			queue.removeFirstOccurrence(new Object());
			fail("Should raise an Exception.");
		} catch (Exception e) {
			assertTrue(e instanceof UnsupportedOperationException);
		}
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#removeLastOccurrence(java.lang.Object)}.
	 * @throws QdbException 
	 */
	@Test
	public void testRemoveLastOccurrence() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueue");
		try {
			queue.removeLastOccurrence(new Object());
			fail("Should raise an Exception.");
		} catch (Exception e) {
			assertTrue(e instanceof UnsupportedOperationException);
		}
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#offer(java.nio.ByteBuffer)}.
	 * @throws QdbException 
	 */
	@Test
	public void testOffer() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueOffer");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        assertTrue(queue.offer(content));
        
        assertTrue(!queue.isEmpty());
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        assertTrue(queue.offer(content));
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        assertTrue(queue.offer(content));
        
        java.nio.ByteBuffer buffer = queue.getLast();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#remove()}.
	 * @throws QdbException 
	 */
	@Test
	public void testRemove() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueue");
		try {
			queue.remove(new Object());
			fail("Should raise an Exception.");
		} catch (Exception e) {
			assertTrue(e instanceof UnsupportedOperationException);
		}
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#poll()}.
	 * @throws QdbException 
	 */
	@Test
	public void testPoll() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueuePoll");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
        
        java.nio.ByteBuffer buffer = queue.poll();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));
		
		buffer = queue.poll();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA2.equals(new String(bytes)));
		
		buffer = queue.poll();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
		
		try {
			assertTrue(queue.poll() == null);
			assertTrue(queue.isEmpty());
		} catch (Exception e) {
			fail("Shouldn't raise an Exception");
		}
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#element()}.
	 * @throws QdbException 
	 */
	@Test
	public void testElement() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueElement");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
        
        java.nio.ByteBuffer buffer = queue.element();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));
		
		buffer = queue.element();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#peek()}.
	 * @throws QdbException 
	 */
	@Test
	public void testPeek() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueElementPeek");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
        
        java.nio.ByteBuffer buffer = queue.peek();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));
		
		buffer = queue.peek();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));
		
		queue.poll();
		queue.poll();
		queue.poll();
		buffer = queue.peek();
		assertTrue(buffer == null);
		assertTrue(queue.isEmpty());
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#push(java.nio.ByteBuffer)}.
	 * @throws QdbException 
	 */
	@Test
	public void testPush() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueuePush");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.push(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.push(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.push(content);
        
        assertTrue(!queue.isEmpty());
        
        java.nio.ByteBuffer buffer = queue.getFirst();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#pop()}.
	 * @throws QdbException 
	 */
	@Test
	public void testPop() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueuePop");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
        
        java.nio.ByteBuffer buffer = queue.pop();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));
		
		buffer = queue.pop();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA2.equals(new String(bytes)));
		
		buffer = queue.pop();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));
		
		try {
			queue.pop();
			fail("Should raise an Exception.");
		} catch (Exception e) {
			assertTrue(e instanceof NoSuchElementException);
			assertTrue(queue.isEmpty());
		}
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#descendingIterator()}.
	 * @throws QdbException 
	 */
	@Test
	public void testDescendingIterator() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueIterator");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
        
        Iterator<java.nio.ByteBuffer> descendingIterator = queue.descendingIterator();
        assertTrue(descendingIterator.hasNext());
        java.nio.ByteBuffer buffer = descendingIterator.next();
        byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA3.equals(new String(bytes)));

		assertTrue(descendingIterator.hasNext());
        buffer = descendingIterator.next();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA2.equals(new String(bytes)));
		
		assertTrue(descendingIterator.hasNext());
        buffer = descendingIterator.next();
        bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA1.equals(new String(bytes)));
		
		assertTrue(!descendingIterator.hasNext());
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#iterator()}.
	 * @throws QdbException 
	 */
	@Test
	public void testIterator() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueIterator");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());

        int i = 0;
        for (java.nio.ByteBuffer buffer : queue) {
        	byte[] bytes = new byte[buffer.limit()];
    		buffer.rewind();
    		buffer.get(bytes, 0, buffer.limit());
    		switch (i) {
    			case 0 : 
    				assertTrue(DATA1.equals(new String(bytes)));
    				break;
    			case 1 :
    				assertTrue(DATA2.equals(new String(bytes)));
    				break;
    			case 2 :
    				assertTrue(DATA3.equals(new String(bytes)));
    				break;
    			default :
    				break;
    		};
    		i++;
        }
	}

	/**
	 * Test method for {@link com.b14.qdb.QdbQueue#add(java.nio.ByteBuffer)}.
	 * @throws QdbException 
	 */
	@Test
	public void testAddByteBuffer() throws QdbException {
		QdbQueue queue = cluster.getQueue("testQueueAddByteBuffer");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA1.getBytes().length);
        content.put(DATA1.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA2.getBytes().length);
        content.put(DATA2.getBytes());
        content.flip();
        queue.add(content);
        
        content = java.nio.ByteBuffer.allocateDirect(DATA3.getBytes().length);
        content.put(DATA3.getBytes());
        content.flip();
        queue.add(content);
        
        assertTrue(!queue.isEmpty());
	}

}
