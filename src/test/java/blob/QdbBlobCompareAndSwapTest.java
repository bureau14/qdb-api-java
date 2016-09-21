import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBlobCompareAndSwapTest {
    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.compareAndSwap(newContent, comparand); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        QdbBlob blob = cluster.blob(alias);
        cluster.close();
        blob.compareAndSwap(newContent, comparand); // <- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbBlob blob = Helpers.getBlob(alias);
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        integer.put(666);
        blob.compareAndSwap(newContent, comparand); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void throwsInvalidArgument_whenExpiryTimeIsInThePast() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime fewMinutesAgo = QdbExpiryTime.makeMinutesFromNow(-7);

        blob.put(content);
        blob.compareAndSwap(content, content, fewMinutesAgo); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        QdbBlob blob = Helpers.getBlob(Helpers.RESERVED_ALIAS);
        blob.compareAndSwap(newContent, comparand); // <- throws
    }

    @Test
    public void returnsNull_whenComparandMatches() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = content.duplicate();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        QdbBuffer result = blob.compareAndSwap(newContent, comparand);

        Assert.assertNull(result);
    }

    @Test
    public void returnsOriginalContent_whenComparandMismatches() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        QdbBuffer result = blob.compareAndSwap(newContent, comparand);

        Assert.assertEquals(content, result.toByteBuffer());
    }
}
