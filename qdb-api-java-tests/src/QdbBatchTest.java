import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.batch.*;
import net.quasardb.qdb.jni.*;
import org.junit.*;

public class QdbBatchTest {
    private QdbBatch batch = null;
    private ByteBuffer content = Helpers.createSampleData();
    private ByteBuffer content_updated = Helpers.createSampleData();

    @Before
    public void setUp() {
        batch = Helpers.createBatch();
    }

    @Test
    public void testBatchMap() {
        Assert.assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_get), TypeOperation.GET);
        Assert.assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_put), TypeOperation.PUT);
        Assert.assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_update), TypeOperation.UPDATE);
        Assert.assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_remove), TypeOperation.REMOVE);
        Assert.assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_cas), TypeOperation.CAS);
        Assert.assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_get_and_update), TypeOperation.GET_UPDATE);
        Assert.assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_get_and_remove), TypeOperation.GET_REMOVE);
        Assert.assertEquals(TypeOperationMap.map(qdb_operation_type_t.qdb_op_blob_remove_if), TypeOperation.REMOVE_IF);

        Assert.assertTrue(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_get));
        Assert.assertFalse(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_put));
        Assert.assertFalse(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_update));
        Assert.assertFalse(OperationHasValue.map(qdb_operation_type_t.qdb_op_remove));
        Assert.assertTrue(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_cas));
        Assert.assertTrue(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_get_and_update));
        Assert.assertTrue(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_get_and_remove));
        Assert.assertFalse(OperationHasValue.map(qdb_operation_type_t.qdb_op_blob_remove_if));
    }

    @Test
    public void testPartialFailure() {
        String alias = Helpers.createUniqueAlias();
        String wrongAlias = Helpers.createUniqueAlias();
        int operationsCount = 0;

        batch.put(alias, content);
        operationsCount++;

        batch.get(wrongAlias);
        operationsCount++;

        batch.get(alias);
        operationsCount++;

        QdbBatchResult results = batch.run();
        Assert.assertFalse(results.isSuccess());
        Assert.assertEquals(results.getOperationsCount(), operationsCount);
        Assert.assertEquals(results.getSuccessesCount(), operationsCount - 1);
        Assert.assertEquals(results.getResultsLength(), operationsCount);

        // check each result one by one
        int index = 0;

        checkBatchResult(results, operationsCount, alias, index++); // put
        // get
        Assert.assertTrue(index < operationsCount);
        Assert.assertEquals(results.get(index).getAlias(), wrongAlias);
        Assert.assertEquals(results.get(index).getError(), qdb_error_t.error_alias_not_found);
        Assert.assertFalse(results.get(index).isSuccess());
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
    public void testUnmatchedContentBatch() {
        String alias = Helpers.createUniqueAlias();
        int operationsCount = 0;

        batch.put(alias, content);
        operationsCount++;

        batch.compareAndSwap(alias, content, /*comparand=*/content_updated);
        operationsCount++;

        QdbBatchResult results = batch.run();
        Assert.assertFalse(results.isSuccess());
        Assert.assertEquals(operationsCount, results.getOperationsCount());
        Assert.assertEquals(1, results.getSuccessesCount());
        Assert.assertEquals(operationsCount, results.getResultsLength());

        // check each result one by one
        int index = 0;

        checkBatchResult(results, operationsCount, alias, index++); // put

        // compare and swap
        Assert.assertTrue(index < operationsCount);
        Assert.assertEquals(alias, results.get(index).getAlias());
        Assert.assertEquals(qdb_error_t.error_unmatched_content, results.get(index).getError());
        Assert.assertFalse(results.get(index).isSuccess());
        ByteBuffer unmatched_content = results.get(index).getValue();
        Assert.assertEquals(content, unmatched_content);
        index++;

        results.release();
    }

    private void createBlob(String alias, ByteBuffer content_to_put) {
        QdbBlob blob = Helpers.getBlob(alias);
        blob.put(content);
    }

    private void checkBlob(String alias, ByteBuffer expected_content) {
        QdbBlob blob = Helpers.getBlob(alias);
        Assert.assertEquals(expected_content, blob.get());
    }

    private void checkBatch(QdbBatchResult results, int operationsCount) {
        Assert.assertTrue(results.isSuccess());
        Assert.assertEquals(results.getOperationsCount(), operationsCount);
        Assert.assertEquals(results.getSuccessesCount(), operationsCount);
        Assert.assertEquals(results.getResultsLength(), operationsCount);
    }

    private void checkBatchResult(QdbBatchResult results, int operationsCount, String alias, int index) {
        checkBatchResult(results, operationsCount, alias, index, /*expected_content=*/null);
    }

    private void checkBatchResult(QdbBatchResult results, int operationsCount, String alias, int index, ByteBuffer expected_content) {
        Assert.assertTrue(index < operationsCount);
        Assert.assertEquals(alias, results.get(index).getAlias());
        Assert.assertEquals(qdb_error_t.error_ok, results.get(index).getError());
        Assert.assertTrue(results.get(index).isSuccess());
        Assert.assertEquals(expected_content, results.get(index).getValue());
    }

    @Test
    public void testSuccessfulBatchGet() {
        String alias = Helpers.createUniqueAlias();
        createBlob(alias, content);

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
    public void testSuccessfulBatchPut() {
        String alias = Helpers.createUniqueAlias();
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
    public void testSuccessfulBatchUpdateCreate() {
        String alias = Helpers.createUniqueAlias();
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
    public void testSuccessfulBatchUpdate() {
        String alias = Helpers.createUniqueAlias();

        createBlob(alias, content);

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
    public void testSuccessfulBatch() {
        String aliasGet = Helpers.createUniqueAlias();
        String aliasPut = Helpers.createUniqueAlias();
        String aliasUpdate = Helpers.createUniqueAlias();
        String aliasUpdateCreate = Helpers.createUniqueAlias();
        String aliasGetAndUpdate = Helpers.createUniqueAlias();
        String aliasRemove = Helpers.createUniqueAlias();
        String aliasCas = Helpers.createUniqueAlias();
        String aliasRemoveIf = Helpers.createUniqueAlias();

        createBlob(aliasGet, content);
        createBlob(aliasUpdate, content);
        createBlob(aliasGetAndUpdate, content);
        createBlob(aliasRemove, content);
        createBlob(aliasRemoveIf, content);
        createBlob(aliasCas, content);

        batch.get(aliasGet);
        batch.put(aliasPut, content);
        batch.update(aliasUpdateCreate, content_updated);
        batch.update(aliasUpdate, content_updated);
        batch.getAndUpdate(aliasGetAndUpdate, content_updated);
        batch.remove(aliasRemove);
        batch.removeIf(aliasRemoveIf, content);
        batch.compareAndSwap(aliasCas, content_updated, content);
        QdbBatchResult results = batch.run();

        // run and check batch
        int operationsCount = 8;
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
