import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBatchBlobCompareAndSwapTest {
    final ByteBuffer originalContent = Helpers.createSampleData();
    final ByteBuffer newContent = Helpers.createSampleData();
    final ByteBuffer comparand = Helpers.createSampleData();
    String alias;
    QdbBlob blob;
    QdbBatch batch;
    QdbFuture<ByteBuffer> result;

    @Before
    public void setUp() {
        alias = Helpers.createUniqueAlias();
        blob = Helpers.getBlob(alias);
        batch = Helpers.createBatch();
    }

    @Test(expected = QdbBatchNotRunException.class)
    public void throwsBatchNotRun_beforeCallingRun() {
        result = batch.blob(alias).compareAndSwap(newContent, comparand);
        result.get(); // <- throw
    }

    @Test(expected = QdbBatchAlreadyRunException.class)
    public void throwsBatchAlreadyRun_afterCallingRun() {
        batch.run();
        batch.blob(alias).compareAndSwap(newContent, comparand); // <- throw
    }

    @Test(expected = QdbBatchClosedException.class)
    public void throwsClosedBatch_afterCallingClose() {
        batch.close();
        batch.blob(alias).compareAndSwap(newContent, comparand); // <- throw
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_whenAliasIsRandom() {
        result = batch.blob(alias).compareAndSwap(newContent, comparand);
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        QdbInteger integer = Helpers.getInteger(alias);

        result = batch.blob(alias).compareAndSwap(newContent, comparand);
        integer.put(666);
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void throwsInvalidArgument_whenExpiryTimeIsInThePast() {
        QdbExpiryTime fiveMinutesAgo = QdbExpiryTime.makeMinutesFromNow(-5);

        result = batch.blob(alias).compareAndSwap(originalContent, originalContent, fiveMinutesAgo);
        blob.put(originalContent);
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenAliasIsQdb() {
        result = batch.blob("qdb").compareAndSwap(newContent, comparand);
        batch.run();
        result.get(); // <- throws
    }

    @Test
    public void returnsNull_whenComparandMatches() {
        result = batch.blob(alias).compareAndSwap(newContent, originalContent);
        blob.put(originalContent);
        batch.run();

        Assert.assertNull(result.get());
    }

    @Test
    public void returnsOriginalContent_whenComparandMismatches() {
        result = batch.blob(alias).compareAndSwap(newContent, comparand);
        blob.put(originalContent);
        batch.run();

        Assert.assertEquals(originalContent, result.get());
    }
}
