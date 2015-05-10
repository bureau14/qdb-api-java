package com.b14.qdb;

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class QdbIteratorIT {
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
            cluster.purgeAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        try {
            cluster.purgeAll();
        } catch (Exception e) {
        }
    }

	/**
     * Test of method {@link QdbCluster#getAllEntries()}.
     * 
     * @throws QdbException
     */
	@Test
	public void testGetAllEntries() throws QdbException {
	    int NB_ITERATIONS = 100;
	    String prefix = "test";
		QdbBlob blob = cluster.getBlob(prefix + "_0");

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
        blob.put(content);
        
        for (int i = 0; i < NB_ITERATIONS; i++) {
            if (i == 0) continue;
            cluster.getBlob(prefix + "_" + i).put(content);
        }
		
        int nbEntries = 0;
        ByteBuffer buffer = null;
        byte[] bytes = null;
        for (QdbEntry entry : cluster.getAllEntries()) {
            assertTrue(entry.getKey().startsWith(prefix));
            buffer = entry.getValue();
            bytes = new byte[buffer.limit()];
            buffer.rewind();
            buffer.get(bytes, 0, buffer.limit());
            assertTrue(DATA.equals(new String(bytes)));
            
            nbEntries++;
        }
        assertTrue(nbEntries == NB_ITERATIONS);
	}

}
