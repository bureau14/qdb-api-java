package com.b14.qdb;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.b14.qdb.batch.QdbBatchResult;
import com.b14.qdb.batch.Result;

import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.jni.qdb_operation_t;
import com.b14.qdb.jni.qdb_operation_type_t;

import com.b14.qdb.batch.TypeOperation;
import com.b14.qdb.batch.TypeOperationMap;
import com.b14.qdb.batch.OperationHasValue;

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

    @Test
    public void testBatchMap() throws QdbException {

        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_get), TypeOperation.GET);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_put), TypeOperation.PUT);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_update), TypeOperation.UPDATE);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_remove), TypeOperation.REMOVE);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_cas), TypeOperation.CAS);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_get_and_update), TypeOperation.GET_UPDATE);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_get_and_remove), TypeOperation.GET_REMOVE);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_remove_if), TypeOperation.REMOVE_IF);

        assertTrue(OperationHasValue.map(qdb_operation_type_t.qdb_op_get));
        assertFalse(OperationHasValue.map(qdb_operation_type_t.qdb_op_put));
        assertFalse(OperationHasValue.map(qdb_operation_type_t.qdb_op_update));
        assertFalse(OperationHasValue.map(qdb_operation_type_t.qdb_op_remove));
        assertTrue(OperationHasValue.map(qdb_operation_type_t.qdb_op_cas));
        assertTrue(OperationHasValue.map(qdb_operation_type_t.qdb_op_get_and_update));
        assertTrue(OperationHasValue.map(qdb_operation_type_t.qdb_op_get_and_remove));
        assertFalse(OperationHasValue.map(qdb_operation_type_t.qdb_op_remove_if));

    }

    @Test
    public void testPartialFailure() throws QdbException {

        java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();

        QdbBatch batch = cluster.createBatch();

        String aliasName = "test_sb";
        String wrongAliasName = "test_sb_2";
        int operationsCount = 0;

        batch.put(aliasName, content);
        operationsCount++;

        batch.get(wrongAliasName);
        operationsCount++;

        batch.get(aliasName);
        operationsCount++;

        QdbBatchResult results = batch.run();
        assertFalse(results.isSuccess());
        assertEquals(results.getOperationsCount(), operationsCount);
        assertEquals(results.getSuccessesCount(), operationsCount - 1);
        assertEquals(results.getResultsLength(), operationsCount);

        // check each result one by one
        int index = 0;

        // put
        assert(index < operationsCount);
        assertEquals(results.get(index).getAlias(), aliasName);
        assertEquals(results.get(index).getError(), qdb_error_t.error_ok);
        assertTrue(results.get(index).isSuccess());
        index++;

        // get
        assert(index < operationsCount);
        assertEquals(results.get(index).getAlias(), wrongAliasName);
        assertEquals(results.get(index).getError(), qdb_error_t.error_alias_not_found);
        assertFalse(results.get(index).isSuccess());
        index++;

        // get
        assert(index < operationsCount);
        assertEquals(results.get(index).getAlias(), aliasName);
        assertEquals(results.get(index).getError(), qdb_error_t.error_ok);
        assertTrue(results.get(index).isSuccess());
        assertEquals(results.get(index).getValue(), content);
        index++;

        results.release();
    }

	/**
     * Test of method {@link QdbCluster#createBatch()}.
     *
     * @throws QdbException
     */
	@Test
	public void testSuccessfulBatch() throws QdbException {

	    java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
        content.put(DATA.getBytes());
        content.flip();
        java.nio.ByteBuffer content_updated = java.nio.ByteBuffer.allocateDirect(DATA_UPDATED.getBytes().length);
        content_updated.put(DATA_UPDATED.getBytes());
        content_updated.flip();

		QdbBatch batch = cluster.createBatch();

        String aliasName = "test_sb";
        int operationsCount = 0;

	    batch.put(aliasName, content);
        operationsCount++;

	    batch.get(aliasName);
        operationsCount++;

	    batch.update(aliasName, content_updated);
        operationsCount++;

	    batch.get(aliasName);
        operationsCount++;

        batch.getAndUpdate(aliasName, content);
        operationsCount++;

	    batch.remove(aliasName);
        operationsCount++;

        batch.update(aliasName, content);
        operationsCount++;

        batch.compareAndSwap(aliasName, content_updated, content);
        operationsCount++;

        batch.removeIf(aliasName, content_updated);
        operationsCount++;

		QdbBatchResult results = batch.run();
		assertTrue(results.isSuccess());
		assertEquals(results.getOperationsCount(), operationsCount);
		assertEquals(results.getSuccessesCount(), operationsCount);
        assertEquals(results.getResultsLength(), operationsCount);

        // check each result one by one
        int index = 0;

        // put
        assert(index < operationsCount);
        assertEquals(results.get(index).getAlias(), aliasName);
        assertEquals(results.get(index).getError(), qdb_error_t.error_ok);
        assertTrue(results.get(index).isSuccess());
        index++;

        // get
        assert(index < operationsCount);
        assertEquals(results.get(index).getAlias(), aliasName);
        assertEquals(results.get(index).getError(), qdb_error_t.error_ok);
        assertTrue(results.get(index).isSuccess());
        assertEquals(results.get(index).getValue(), content);
        index++;

        // update
        assert(index < operationsCount);
        assertEquals(results.get(index).getAlias(), aliasName);
        assertEquals(results.get(index).getError(), qdb_error_t.error_ok);
        assertTrue(results.get(index).isSuccess());
        index++;

        // get
        assert(index < operationsCount);
        assertEquals(results.get(index).getAlias(), aliasName);
        assertEquals(results.get(index).getError(), qdb_error_t.error_ok);
        assertTrue(results.get(index).isSuccess());
        assertEquals(results.get(index).getValue(), content_updated);
        index++;

        // get and update
        assert(index < operationsCount);
        assertEquals(results.get(index).getAlias(), aliasName);
        assertEquals(results.get(index).getError(), qdb_error_t.error_ok);
        assertTrue(results.get(index).isSuccess());
        assertEquals(results.get(index).getValue(), content_updated);
        index++;

        // remove
        assert(index < operationsCount);
        assertEquals(results.get(index).getAlias(), aliasName);
        assertEquals(results.get(index).getError(), qdb_error_t.error_ok);
        assertTrue(results.get(index).isSuccess());
        index++;

        // update
        assert(index < operationsCount);
        assertEquals(results.get(index).getAlias(), aliasName);
        assertEquals(results.get(index).getError(), qdb_error_t.error_ok);
        assertTrue(results.get(index).isSuccess());
        index++;

        // compare and swap
        assert(index < operationsCount);
        assertEquals(results.get(index).getAlias(), aliasName);
        assertEquals(results.get(index).getError(), qdb_error_t.error_ok);
        assertTrue(results.get(index).isSuccess());
        assertEquals(results.get(index).getValue(), content);
        index++;

        // remove if
        assert(index < operationsCount);
        assertEquals(results.get(index).getAlias(), aliasName);
        assertTrue(results.get(index).isSuccess());
        assertEquals(results.get(index).getError(), qdb_error_t.error_ok);
        assertTrue(results.get(index).isSuccess());
        index++;

        results.release();
	}
}
