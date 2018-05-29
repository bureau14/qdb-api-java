import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;
import org.junit.*;

public class QdbBatchMultipleOperationsTest {
    final ByteBuffer content = Helpers.createSampleData();
    final ByteBuffer content_updated = Helpers.createSampleData();
    QdbBatch batch;

    @Before
    public void setUp() {
        batch = Helpers.createBatch();
    }

    @After
    public void cleanUp() {
        batch.close();
    }

    @Test
    public void testPartialFailure() {
        String alias = Helpers.createUniqueAlias();
        String wrongAlias = Helpers.createUniqueAlias();

        QdbFuture<Void> result1 = batch.blob(alias).put(content);
        QdbFuture<ByteBuffer> result2 = batch.blob(wrongAlias).get();
        QdbFuture<ByteBuffer> result3 = batch.blob(alias).get();

        batch.run();

        Assert.assertFalse(batch.success());
        Assert.assertEquals(batch.operationCount(), 3);
        Assert.assertEquals(batch.successCount(), 2);

        Assert.assertTrue(result1.success());
        Assert.assertFalse(result2.success());
        Assert.assertEquals(content, result3.get());
    }

    @Test
    public void testUnmatchedContentBatch() {
        String alias = Helpers.createUniqueAlias();

        QdbFuture<Void> result1 = batch.blob(alias).put(content);
        QdbFuture<ByteBuffer> result2 = batch.blob(alias).compareAndSwap(content, /*comparand=*/content_updated);

        batch.run();
        Assert.assertFalse(batch.success());
        Assert.assertEquals(2, batch.operationCount());
        Assert.assertEquals(1, batch.successCount());

        Assert.assertTrue(result1.success());
        Assert.assertTrue(result2.success());
        Assert.assertEquals(content, result2.get());
    }

    private void createBlob(String alias, ByteBuffer content_to_put) {
        QdbBlob blob = Helpers.getBlob(alias);
        blob.put(content);
    }

    private void checkBlob(String alias, ByteBuffer expected_content) {
        QdbBlob blob = Helpers.getBlob(alias);
        Assert.assertEquals(expected_content, blob.get().toByteBuffer());
    }

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

        QdbFuture<ByteBuffer> resultOfGet = batch.blob(aliasGet).get();
        QdbFuture<Void> resultOfPut = batch.blob(aliasPut).put(content);
        QdbFuture<Void> resultOfUpdateCreate = batch.blob(aliasUpdateCreate).update(content_updated);
        QdbFuture<Void> resultOfUpdate = batch.blob(aliasUpdate).update(content_updated);
        QdbFuture<ByteBuffer> resultOfGetAndUpdate = batch.blob(aliasGetAndUpdate).getAndUpdate(content_updated);
        QdbFuture<ByteBuffer> resultOfCompareAndSwap = batch.blob(aliasCas).compareAndSwap(content_updated, content);

        batch.run();
        Assert.assertTrue(batch.success());
        Assert.assertEquals(batch.operationCount(), 6);
        Assert.assertEquals(batch.successCount(), 6);

        // check each result one by one
        Assert.assertEquals(content, resultOfGet.get());
        Assert.assertTrue(resultOfPut.success());
        checkBlob(aliasPut, content);
        Assert.assertTrue(resultOfUpdateCreate.success());
        checkBlob(aliasUpdateCreate, content_updated);
        Assert.assertTrue(resultOfUpdate.success());
        checkBlob(aliasUpdate, content_updated);
        Assert.assertEquals(content, resultOfGetAndUpdate.get());
        checkBlob(aliasGetAndUpdate, content_updated);
        Assert.assertNull(resultOfCompareAndSwap.get());
        checkBlob(aliasCas, content_updated);
    }
}
