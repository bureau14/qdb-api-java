import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBlobGetTest {
    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingCompareAndSwap_thenWaitingExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = content.duplicate();
        ByteBuffer newContent = Helpers.createSampleData();
        QdbExpiryTime expiry = QdbExpiryTime.makeSecondsFromNow(1);

        blob.put(content);
        blob.compareAndSwap(newContent, comparand, expiry);
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingSetExpiryTime_thenWaitingExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime expiry = QdbExpiryTime.makeSecondsFromNow(1);

        blob.put(content);
        blob.expiryTime(expiry);
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingGetAndRemove() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        blob.getAndRemove();
        blob.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingGetAndUpdate_thenWaitingExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime expiry = QdbExpiryTime.makeSecondsFromNow(1);

        blob.put(content);
        blob.getAndUpdate(content, expiry);
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingPut_thenWaitingExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime expiry = QdbExpiryTime.makeSecondsFromNow(1);

        blob.put(content, expiry);
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingRemove() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        blob.remove();
        blob.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingUpdate_thenWaitingExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime expiry = QdbExpiryTime.makeSecondsFromNow(1);

        blob.put(content);
        blob.update(content, expiry);
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        ByteBuffer content = Helpers.createSampleData();
        String alias = Helpers.createUniqueAlias();

        QdbBlob blob = cluster.blob(alias);
        cluster.close();
        blob.get(); // <- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbBlob blob = Helpers.getBlob(alias);

        integer.put(666);
        blob.get(); // <- throws
    }

    @Test
    public void returnsOriginalContent_afterCallingBatchPut() {
        String alias = Helpers.createUniqueAlias();
        QdbBatch batch = Helpers.createBatch();
        QdbBlob blob = Helpers.getBlob(alias);
        ByteBuffer content = Helpers.createSampleData();

        batch.blob(alias).put(content);
        batch.run();
        QdbBuffer result = blob.get();

        Assert.assertEquals(content, result.toByteBuffer());
    }

    @Test
    public void returnsOriginalContent_afterCallingBatchUpdate() {
        String alias = Helpers.createUniqueAlias();
        QdbBatch batch = Helpers.createBatch();
        QdbBlob blob = Helpers.getBlob(alias);
        ByteBuffer content = Helpers.createSampleData();

        batch.blob(alias).update(content);
        batch.run();
        QdbBuffer result = blob.get();

        Assert.assertEquals(content, result.toByteBuffer());
    }

    @Test
    public void returnsOriginalContent_afterCallingPut() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        QdbBuffer result = blob.get();

        Assert.assertEquals(content, result.toByteBuffer());
    }

    @Test
    public void returnsOriginalContent_afterCallingCompareAndSwap_whenComparandMismatches() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        blob.compareAndSwap(newContent, comparand);
        QdbBuffer result = blob.get();

        Assert.assertEquals(content, result.toByteBuffer());
    }

    @Test
    public void returnsUpdatedContent_afterCallingGetAndUpdate() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        blob.put(content1);
        blob.getAndUpdate(content2);
        QdbBuffer result = blob.get();

        Assert.assertEquals(content2, result.toByteBuffer());
    }

    @Test
    public void returnsUpdatedContent_afterCallingUpdate() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        blob.update(newContent);
        QdbBuffer result = blob.get();

        Assert.assertEquals(newContent, result.toByteBuffer());
    }

    @Test
    public void returnsUpdatedContent_afterCallingCompareAndSwap_whenComparandMatches() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = content.duplicate();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        blob.compareAndSwap(newContent, comparand);
        QdbBuffer result = blob.get();

        Assert.assertEquals(newContent, result.toByteBuffer());
    }
}
