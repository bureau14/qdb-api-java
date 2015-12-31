package net.quasardb.qdb;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.quasardb.qdb.batch.QdbBatchResult;
import net.quasardb.qdb.batch.Result;

import net.quasardb.qdb.jni.qdb_error_t;
import net.quasardb.qdb.jni.qdb_operation_t;
import net.quasardb.qdb.jni.qdb_operation_type_t;

import net.quasardb.qdb.batch.TypeOperation;
import net.quasardb.qdb.batch.TypeOperationMap;
import net.quasardb.qdb.batch.OperationHasValue;

public class QdbBatchTest {
    private static final String DATA = "This is my data test";
    private static final String DATA_UPDATED = "This is my new data test";

    private static int aliasCounter = 0;

    private QdbCluster cluster = null;
    private QdbBatch batch = null;
    private java.nio.ByteBuffer content = null;
    private java.nio.ByteBuffer content_updated = null;

    private String getUniqueAlias() {
        return "alias" + Integer.toString(++aliasCounter);
    }

    @Before
    public void setUp() {
        try {
            cluster = new QdbCluster(DaemonRunner.getURI());

            content = java.nio.ByteBuffer.allocateDirect(DATA.getBytes().length);
            content.put(DATA.getBytes());
            content.flip();

            content_updated = java.nio.ByteBuffer.allocateDirect(DATA_UPDATED.getBytes().length);
            content_updated.put(DATA_UPDATED.getBytes());
            content_updated.flip();

            batch = cluster.createBatch();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testBatchMap() throws QdbException {
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_get), TypeOperation.GET);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_put), TypeOperation.PUT);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_update), TypeOperation.UPDATE);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_remove), TypeOperation.REMOVE);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_cas), TypeOperation.CAS);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_get_and_update), TypeOperation.GET_UPDATE);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_get_and_remove), TypeOperation.GET_REMOVE);
        assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_remove_if), TypeOperation.REMOVE_IF);

        assertTrue(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_get));
        assertFalse(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_put));
        assertFalse(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_update));
        assertFalse(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_remove));
        assertTrue(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_cas));
        assertTrue(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_get_and_update));
        assertTrue(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_get_and_remove));
        assertFalse(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_remove_if));
    }

    @Test
    public void testPartialFailure() throws QdbException {
        String alias = getUniqueAlias();
        String wrongAlias = getUniqueAlias();
        int operationsCount = 0;

        batch.put(alias, content);
        operationsCount++;

        batch.get(wrongAlias);
        operationsCount++;

        batch.get(alias);
        operationsCount++;

        QdbBatchResult results = batch.run();
        assertFalse(results.isSuccess());
        assertEquals(results.getOperationsCount(), operationsCount);
        assertEquals(results.getSuccessesCount(), operationsCount - 1);
        assertEquals(results.getResultsLength(), operationsCount);

        // check each result one by one
        int index = 0;

        checkBatchResult(results, operationsCount, alias, index++); // put
        // get
        assert(index < operationsCount);
        assertEquals(results.get(index).getAlias(), wrongAlias);
        assertEquals(results.get(index).getError(), qdb_error_t.error_alias_not_found);
        assertFalse(results.get(index).isSuccess());
        index++;
        checkBatchResult(results, operationsCount, alias, index++, content); // get

        results.release();
    }

    /**
     * Test of method {@link QdbCluster#createBatch()}, failure in {@link QdbBatch#compareAndSwap()}.
     *
     * @throws QdbException
     */
    @Test
    public void testUnmatchedContentBatch() throws QdbException {
        String alias = getUniqueAlias();
        int operationsCount = 0;

        batch.put(alias, content);
        operationsCount++;

        batch.compareAndSwap(alias, content, /*comparand=*/content_updated);
        operationsCount++;

        QdbBatchResult results = batch.run();
        assertFalse(results.isSuccess());
        assertEquals(operationsCount, results.getOperationsCount());
        assertEquals(1, results.getSuccessesCount());
        assertEquals(operationsCount, results.getResultsLength());

        // check each result one by one
        int index = 0;

        checkBatchResult(results, operationsCount, alias, index++); // put

        // compare and swap
        assert(index < operationsCount);
        assertEquals(alias, results.get(index).getAlias());
        assertEquals(qdb_error_t.error_unmatched_content, results.get(index).getError());
        assertFalse(results.get(index).isSuccess());
        java.nio.ByteBuffer unmatched_content = results.get(index).getValue();
        assertNotNull(unmatched_content);
        byte[] bytes = new byte[unmatched_content.limit()];
        unmatched_content.rewind();
        unmatched_content.get(bytes, 0, unmatched_content.limit());
        assertTrue(DATA.equals(new String(bytes)));
        index++;

        results.release();
    }

    private void createBlob(String alias, java.nio.ByteBuffer content_to_put) throws QdbException {
        QdbBlob blob = cluster.getBlob(alias);
        blob.put(content);

        checkBlob(alias, content_to_put);
    }

    private void checkBlob(String alias, java.nio.ByteBuffer expected_content) throws QdbException {
        QdbBlob blob = cluster.getBlob(alias);
        assertEquals(expected_content, blob.get());
    }

    private void checkBatch(QdbBatchResult results, int operationsCount) {
        assertTrue(results.isSuccess());
        assertEquals(results.getOperationsCount(), operationsCount);
        assertEquals(results.getSuccessesCount(), operationsCount);
        assertEquals(results.getResultsLength(), operationsCount);
    }

    private void checkBatchResult(QdbBatchResult results, int operationsCount, String alias, int index) {
        checkBatchResult(results, operationsCount, alias, index, /*expected_content=*/null);
    }

    private void checkBatchResult(QdbBatchResult results, int operationsCount, String alias, int index, java.nio.ByteBuffer expected_content) {
        assert(index < operationsCount);
        assertEquals(alias, results.get(index).getAlias());
        assertEquals(qdb_error_t.error_ok, results.get(index).getError());
        assertTrue(results.get(index).isSuccess());
        assertEquals(expected_content, results.get(index).getValue());
    }

    /**
     * Test of method {@link QdbBatch#get()}.
     *
     * @throws QdbException
     */
    @Test
    public void testSuccessfulBatchGet() throws QdbException {
        String alias = getUniqueAlias();

        createBlob(alias, content);
        checkBlob(alias, content);

        int operationsCount = 0;

        batch.get(alias);
        operationsCount++;

        // run and check batch
        QdbBatchResult results = batch.run();
        checkBatch(results, operationsCount);

        int index = 0;
        checkBatchResult(results, operationsCount, alias, index++, content); // get

        checkBlob(alias, content);

        results.release();
    }

    /**
     * Test of method {@link QdbBatch#put()}.
     *
     * @throws QdbException
     */
    @Test
    public void testSuccessfulBatchPut() throws QdbException {
        String alias = getUniqueAlias();
        int operationsCount = 0;

        batch.put(alias, content);
        operationsCount++;

        // run and check batch
        QdbBatchResult results = batch.run();
        checkBatch(results, operationsCount);

        int index = 0;
        checkBatchResult(results, operationsCount, alias, index++); // put

        checkBlob(alias, content);

        results.release();
    }

    /**
     * Test of method {@link QdbBatch#update()}, entry does not exist yet.
     *
     * @throws QdbException
     */
    @Test
    public void testSuccessfulBatchUpdateCreate() throws QdbException {
        String alias = getUniqueAlias();
        int operationsCount = 0;

        batch.update(alias, content_updated);
        operationsCount++;

        // run and check batch
        QdbBatchResult results = batch.run();
        checkBatch(results, operationsCount);

        int index = 0;
        checkBatchResult(results, operationsCount, alias, index++); // update

        checkBlob(alias, content_updated);

        results.release();
    }

    /**
     * Test of method {@link QdbBatch#update()}, entry already exists.
     *
     * @throws QdbException
     */
    @Test
    public void testSuccessfulBatchUpdate() throws QdbException {
        String alias = getUniqueAlias();

        createBlob(alias, content);
        checkBlob(alias, content);

        int operationsCount = 0;

        batch.update(alias, content_updated);
        operationsCount++;

        // run and check batch
        QdbBatchResult results = batch.run();
        checkBatch(results, operationsCount);

        int index = 0;
        checkBatchResult(results, operationsCount, alias, index++); // update

        checkBlob(alias, content_updated);

        results.release();
    }

    /**
     * Test of method {@link QdbCluster#createBatch()}.
     *
     * @throws QdbException
     */
    @Test
    public void testSuccessfulBatch() throws QdbException {
        String aliasGet = getUniqueAlias();
        String aliasPut = getUniqueAlias();
        String aliasUpdate = getUniqueAlias();
        String aliasUpdateCreate = getUniqueAlias();
        String aliasGetAndUpdate = getUniqueAlias();
        String aliasRemove = getUniqueAlias();
        String aliasCas = getUniqueAlias();
        String aliasRemoveIf = getUniqueAlias();

        createBlob(aliasGet, content);
        checkBlob(aliasGet, content);

        createBlob(aliasUpdate, content);
        checkBlob(aliasUpdate, content);

        createBlob(aliasGetAndUpdate, content);
        checkBlob(aliasGetAndUpdate, content);

        createBlob(aliasRemove, content);
        checkBlob(aliasRemove, content);

        createBlob(aliasRemoveIf, content);
        checkBlob(aliasRemoveIf, content);

        createBlob(aliasCas, content);
        checkBlob(aliasCas, content);

        int operationsCount = 0;

        batch.get(aliasGet);
        operationsCount++;

        batch.put(aliasPut, content);
        operationsCount++;

        batch.update(aliasUpdateCreate, content_updated);
        operationsCount++;

        batch.update(aliasUpdate, content_updated);
        operationsCount++;

        batch.getAndUpdate(aliasGetAndUpdate, content_updated);
        operationsCount++;

        batch.remove(aliasRemove);
        operationsCount++;

        batch.removeIf(aliasRemoveIf, content);
        operationsCount++;

        batch.compareAndSwap(aliasCas, content_updated, content);
        operationsCount++;

        // run and check batch
        QdbBatchResult results = batch.run();
        checkBatch(results, operationsCount);

        // check each result one by one
        int index = 0;
        checkBatchResult(results, operationsCount, aliasGet, index++, content);
        checkBatchResult(results, operationsCount, aliasPut, index++);
        checkBlob(aliasPut, content);
        checkBatchResult(results, operationsCount, aliasUpdateCreate, index++);
        checkBlob(aliasUpdateCreate, content_updated);
        checkBatchResult(results, operationsCount, aliasUpdate, index++);
        checkBlob(aliasUpdate, content_updated);
        checkBatchResult(results, operationsCount, aliasGetAndUpdate, index++, content);
        checkBlob(aliasGetAndUpdate, content_updated);
        checkBatchResult(results, operationsCount, aliasRemove, index++);
        checkBatchResult(results, operationsCount, aliasRemoveIf, index++);
        checkBatchResult(results, operationsCount, aliasCas, index++);
        checkBlob(aliasCas, content_updated);

        results.release();
    }
}
