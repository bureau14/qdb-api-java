package com.b14.qdb;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.b14.qdb.batch.QdbBatchResult;
import com.b14.qdb.batch.Result;

public class QdbBatchIT {
	private static final String URI = "qdb://127.0.0.1:2836";
	private static final String DATA = "This is my data test";
	private static final String DATA_UPDATED = "This is my new data test";
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
     * Test of method {@link QdbCluster#startBatch()}.
     * 
     * @throws QdbException
     */
	@Test
	public void testStartBatch() throws QdbException {
	    int NB_ITERATIONS = 10;
	    String prefix = "test_";
	    java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
        java.nio.ByteBuffer content_updated = java.nio.ByteBuffer.allocateDirect(DATA_UPDATED.getBytes().length);
        content_updated.put(DATA_UPDATED.getBytes());
        content_updated.flip();
        
		QdbBatch batch = cluster.startBatch();
		for (int i = 0; i < NB_ITERATIONS; i++) {
		    batch.put("test_" + i, content);
		    batch.get("test_" + i);
		    batch.update("test_" + i, content_updated);
		    batch.get("test_" + i);
		    batch.remove("test_" + i);
		}
		QdbBatchResult results = batch.run();
		assertTrue(results.isSuccess());
		assertTrue(results.getNbOperations() == (NB_ITERATIONS * 5));
		assertTrue(results.getNbSuccess() == (NB_ITERATIONS * 5));
        
		for (Result result : results.getResults()) {
		    assertTrue(result.getAlias().startsWith(prefix));
		    assertTrue(result.isSuccess());
		}
	}
}
