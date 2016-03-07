import java.nio.ByteBuffer;
import java.util.Date;
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
        QdbExpiryTime fiveMinutesAgo = QdbExpiryTime.makeMinutesFromNow(-5);

        blob.put(content);
        blob.compareAndSwap(content, content, fiveMinutesAgo); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        QdbBlob blob = Helpers.getBlob("qdb");
        blob.compareAndSwap(newContent, comparand); // <- throws
    }

    @Test
    public void returnsNull_whenComparandMatches() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = content.duplicate();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        ByteBuffer result = blob.compareAndSwap(newContent, comparand);

        Assert.assertNull(result);
    }

    @Test
    public void returnsOriginalContent_whenComparandMismatches() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        ByteBuffer result = blob.compareAndSwap(newContent, comparand);

        Assert.assertEquals(content, result);
    }
}
