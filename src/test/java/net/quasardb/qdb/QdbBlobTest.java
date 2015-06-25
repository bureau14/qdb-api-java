package net.quasardb.qdb;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class QdbBlobTest {
	private static final String URI = "qdb://127.0.0.1:2836";
	private static final String DATA = "This is my data test";
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
    }

    /**
     * Test of method {@link QdbBlob#alias()}.
     * 
     * @throws QdbException
     */
	@Test
	public void testAlias() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");
		assertTrue("testBlob".equals(blob.getAlias()));
	}

	/**
     * Test of method {@link QdbBlob#get()}.
     * 
     * @throws QdbException
     */
	@Test
	public void testGet() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.update(content);
		
		java.nio.ByteBuffer buffer = blob.get();
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		cleanUp(blob);
	}

	/**
     * Test of method {@link QdbBlob#getAndRemove()}.
     * 
     * @throws QdbException
     */
	@Test
	public void testGetAndRemove() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.update(content);
		
		java.nio.ByteBuffer buffer = blob.getAndRemove();
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		try {
			blob.get();
			fail("Should raise an exception.");
		} catch (Exception e) {
			assertTrue(e instanceof QdbException);
		}
		
		cleanUp(blob);
	}

	/**
     * Test of method {@link QdbBlob#getAndUpdate(java.nio.ByteBuffer)}.
     * 
     * @throws QdbException
     */
	@Test
	public void testGetAndUpdateByteBuffer() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.update(content);
		
		content.clear();
		content = java.nio.ByteBuffer.allocateDirect((DATA + "UPDATE").getBytes().length);
		content.put((DATA + "UPDATE").getBytes());
        content.flip();
		java.nio.ByteBuffer buffer = blob.getAndUpdate(content);
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		buffer = blob.get();
		bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue((DATA + "UPDATE").equals(new String(bytes)));
        
		cleanUp(blob);
	}

	/**
     * Test of method {@link QdbBlob#getAndUpdate(java.nio.ByteBuffer, long)}.
     * 
     * @throws QdbException
     */
	@Test
	public void testGetAndUpdateByteBufferLong() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.update(content);
		
		content.clear();
		content = java.nio.ByteBuffer.allocateDirect((DATA + "UPDATE").getBytes().length);
		content.put((DATA + "UPDATE").getBytes());
        content.flip();
		java.nio.ByteBuffer buffer = blob.getAndUpdate(content, 1);
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		buffer = blob.get();
		bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue((DATA + "UPDATE").equals(new String(bytes)));
		
		try {
			Thread.sleep(1500L);
		} catch (Exception e) {
		}
		
		try {
			blob.get();
			fail("Should raise an Exception.");
		} catch (Exception e) {
			assertTrue(e instanceof QdbException);
		}
        
		cleanUp(blob);
	}

	/**
     * Test of method {@link QdbBlob#put(java.nio.ByteBuffer)}.
     * 
     * @throws QdbException
     */
	@Test
	public void testPutByteBuffer() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.put(content);
		
		java.nio.ByteBuffer buffer = blob.get();
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		cleanUp(blob);
	}
	
	/**
     * Test of method {@link QdbBlob#put(java.nio.ByteBuffer, long)}.
     * 
     * @throws QdbException
     */
	@Test
	public void testPutByteBufferLong() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.put(content, 1);
		
		java.nio.ByteBuffer buffer = blob.get();
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		try {
			Thread.sleep(1500L);
		} catch (Exception e) {
		}
		
		try {
			blob.get();
			fail("Should raise an Exception.");
		} catch (Exception e) {
			assertTrue(e instanceof QdbException);
		}
		
		cleanUp(blob);
	}

	@Test
	public void testCompareAndSwapByteBufferByteBufferTrue() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.update(content);
		
		java.nio.ByteBuffer buffer = blob.get();
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		java.nio.ByteBuffer comparand = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
		comparand.put(DATA.getBytes());
		comparand.flip();
		
		java.nio.ByteBuffer newContent = java.nio.ByteBuffer.allocateDirect((DATA + " NEW").getBytes().length);
		newContent.put((DATA + " NEW").getBytes());
		newContent.flip();
		
		buffer = blob.compareAndSwap(newContent, comparand);
		bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		buffer = blob.get();
		bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue((DATA + " NEW").equals(new String(bytes)));
		
		cleanUp(blob);
	}
	
	@Test
	public void testCompareAndSwapByteBufferByteBufferFalse() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.put(content);
		
		java.nio.ByteBuffer buffer = blob.get();
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		java.nio.ByteBuffer comparand = java.nio.ByteBuffer.allocateDirect("different".getBytes().length);
		comparand.put("different".getBytes());
		comparand.flip();
		
		java.nio.ByteBuffer newContent = java.nio.ByteBuffer.allocateDirect((DATA + " NEW").getBytes().length);
		newContent.put((DATA + " NEW").getBytes());
		newContent.flip();
		
		buffer = blob.compareAndSwap(newContent, comparand);
		bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		buffer = blob.get();
		bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		cleanUp(blob);
	}

	@Test
	public void testCompareAndSwapByteBufferByteBufferLong() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.put(content);
		
		java.nio.ByteBuffer buffer = blob.get();
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		java.nio.ByteBuffer comparand = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
		comparand.put(DATA.getBytes());
		comparand.flip();
		
		java.nio.ByteBuffer newContent = java.nio.ByteBuffer.allocateDirect((DATA + " NEW").getBytes().length);
		newContent.put((DATA + " NEW").getBytes());
		newContent.flip();
		
		buffer = blob.compareAndSwap(newContent, comparand, 1L);
		bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		buffer = blob.get();
		bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue((DATA + " NEW").equals(new String(bytes)));
		
		try {
			Thread.sleep(1500L);
		} catch (Exception e) {
		}
		
		try {
			blob.get();
			fail("Should raise an Exception.");
		} catch (Exception e) {
			assertTrue(e instanceof QdbException);
		}
		
		cleanUp(blob);
	}

	/**
     * Test of method {@link QdbBlob#update(java.nio.ByteBuffer)}.
     * 
     * @throws QdbException
     */
	@Test
	public void testUpdateByteBuffer() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.update(content);
		
		java.nio.ByteBuffer buffer = blob.get();
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		content.clear();
		content = java.nio.ByteBuffer.allocateDirect((DATA + "UPDATE").getBytes().length);
		content.put((DATA + "UPDATE").getBytes());
        content.flip();
        blob.update(content);
        buffer = blob.get();
		bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertFalse(DATA.equals(new String(bytes)));
		assertTrue((DATA + "UPDATE").equals(new String(bytes)));
        
		cleanUp(blob);
	}

	/**
     * Test of method {@link QdbBlob#update(java.nio.ByteBuffer, long)}.
     * 
     * @throws QdbException
     */
	@Test
	public void testUpdateByteBufferLong() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.put(content);
		
		java.nio.ByteBuffer buffer = blob.get();
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		content.clear();
		content = java.nio.ByteBuffer.allocateDirect((DATA + "UPDATE").getBytes().length);
		content.put((DATA + "UPDATE").getBytes());
        content.flip();
        blob.update(content, 1L);
        buffer = blob.get();
		bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertFalse(DATA.equals(new String(bytes)));
		assertTrue((DATA + "UPDATE").equals(new String(bytes)));
		
		try {
			Thread.sleep(1500L);
		} catch (Exception e) {
		}
		
		try {
			blob.get();
			fail("Should raise an Exception.");
		} catch (Exception e) {
			assertTrue(e instanceof QdbException);
		}
        
		cleanUp(blob);
	}

	/**
     * Test of method {@link QdbBlob#expiresAt(Date)}.
     * 
     * @throws QdbException
     */
	@Test
	public void testExpiresAt() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.put(content);
		
		long time = System.currentTimeMillis() + 500;
        Date expiryDate = new Date(time);
        blob.expiresAt(expiryDate);
        
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
		}
		
		try {
			blob.get();
			fail("Should raise an Exception");
		} catch (Exception e) {
			assertTrue(e instanceof QdbException);
		}
	}

	/**
     * Test of method {@link QdbBlob#expiresFromNow(long)}.
     * 
     * @throws QdbException
     */
	@Test
	public void testExpiresFromNow() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");
		java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.put(content);
		
		java.nio.ByteBuffer buffer = blob.get();
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		blob.expiresFromNow(0);
		
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
		}
		
        try {
        	blob.get();
            fail("Entry should have been expired");
        } catch (Exception e) {
            assertTrue(e instanceof QdbException);
        }
	}

	/**
     * Test of method {@link QdbBlob#getExpiryTime()}.
     * 
     * @throws QdbException
     */
	@Test
	public void testGetExpiryTime() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.put(content);
		
		try {
			long time = System.currentTimeMillis() + (1000 * 60 * 60);
	        Date expiryDate = new Date(time);
	        blob.expiresAt(expiryDate);
	        
	        String sTime = "" + time;
	        assertTrue("Expiry time for entry[test_expiry_1] should be equals to computed date => " + sTime.substring(0, sTime.length() - 3), (blob.getExpiryTime() + "").equals(sTime.substring(0, sTime.length() - 3)));
		} finally {
			cleanUp(blob);
		}
	}

	/**
     * Test of method {@link QdbBlob#remove()}.
     * 
     * @throws QdbException
     */
	@Test
	public void testRemove() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.put(content);
		
		java.nio.ByteBuffer buffer = blob.get();
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		blob.remove();
		buffer = null;
		try {
			buffer = blob.get();
			fail("Should raise an Exception");
		} catch (Exception e) {
			assertTrue(e instanceof QdbException);
			assertTrue(buffer == null);
		}
	}

	/**
     * Test of method {@link QdbBlob#removeIf(java.nio.ByteBuffer)}.
     * 
     * @throws QdbException
     */
	@Test
	public void testRemoveIfTrue() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.put(content);
		
		java.nio.ByteBuffer buffer = blob.get();
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		java.nio.ByteBuffer comparand = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
		comparand.put(DATA.getBytes());
		comparand.flip();
		assertTrue(blob.removeIf(comparand));
		
		cleanUp(blob);
	}

	/**
     * Test of method {@link QdbBlob#removeIf(java.nio.ByteBuffer)}.
     * 
     * @throws QdbException
     */
	@Test
	public void testRemoveIfFalse() throws QdbException {
		QdbBlob blob = cluster.getBlob("testBlob");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
		blob.update(content);
		
		java.nio.ByteBuffer buffer = blob.get();
		byte[] bytes = new byte[buffer.limit()];
		buffer.rewind();
		buffer.get(bytes, 0, buffer.limit());
		assertTrue(DATA.equals(new String(bytes)));
		
		java.nio.ByteBuffer comparand = java.nio.ByteBuffer.allocateDirect("test_false".getBytes().length);
		comparand.put("test_false".getBytes());
		comparand.flip();
		assertFalse(blob.removeIf(comparand));
		
		cleanUp(blob);
	}

    @Test
    public void testTag() throws QdbException {

        String myBlob = "taggedBlob";

        QdbBlob blob = cluster.getBlob(myBlob);

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
        blob.update(content);
        
        String myTag = "Boom";

        assertFalse(blob.hasTag(myTag));

        // cannot add the tag twice
        assertTrue(blob.addTag(myTag));
        assertFalse(blob.addTag(myTag));

        // tag must be present
        assertTrue(blob.hasTag(myTag));

        // tag must be listed
        List<String> tags = blob.getTags();

        assertEquals(tags.size(), 1);
        assertEquals(tags.get(0), myTag);

        // reverse lookup must work
        QdbTag tag = cluster.getTag(myTag);

        List<String> entries = tag.getEntries();

        assertEquals(entries.size(), 1);
        assertEquals(entries.get(0), myBlob);

        // cannot remove tag twice
        assertTrue(blob.removeTag(myTag));
        assertFalse(blob.removeTag(myTag));

        assertFalse(blob.hasTag(myTag));

        assertEquals(entries.size(), 1);
        assertEquals(entries.get(0), myBlob); 

        cleanUp(blob);
    }
	
	private void cleanUp(QdbBlob blob) {
		try {
			blob.remove();
		} catch (Exception e) {
			
		}
	}
}
