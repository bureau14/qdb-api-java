import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbBlobCompareAndSwapTest {
    @Test(expected = AliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.compareAndSwap(newContent, comparand); // <- throws
    }

    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        QdbBlob blob = cluster.blob(alias);
        cluster.close();
        blob.compareAndSwap(newContent, comparand); // <- throws
    }

    @Test(expected = IncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbBlob blob = Helpers.getBlob(alias);
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        integer.put(666);
        blob.compareAndSwap(newContent, comparand); // <- throws
    }

    @Test(expected = InvalidArgumentException.class)
    public void throwsInvalidArgument_whenExpiryTimeIsInThePast() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime fewMinutesAgo = QdbExpiryTime.makeMinutesFromNow(-7);

        blob.put(content);
        blob.compareAndSwap(content, content, fewMinutesAgo); // <- throws
    }

    @Test(expected = ReservedAliasException.class)
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
        Buffer result = blob.compareAndSwap(newContent, comparand);

        Assert.assertNull(result);
    }

    @Test
    public void returnsOriginalContent_whenComparandMismatches() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        Buffer result = blob.compareAndSwap(newContent, comparand);

        Assert.assertEquals(content, result.toByteBuffer());
    }
}
