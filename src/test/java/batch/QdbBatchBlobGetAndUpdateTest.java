import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBatchBlobGetAndUpdateTest {
    final ByteBuffer originalContent = Helpers.createSampleData();
    final ByteBuffer newContent = Helpers.createSampleData();
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
        result = batch.blob(alias).getAndUpdate(newContent);
        result.get(); // <- throw
    }

    @Test(expected = QdbBatchAlreadyRunException.class)
    public void throwsBatchAlreadyRun_afterCallingRun() {
        batch.run();
        batch.blob(alias).getAndUpdate(newContent); // <- throw
    }

    @Test(expected = QdbBatchClosedException.class)
    public void throwsBatchClosed_afterCallingClose() {
        batch.close();
        batch.blob(alias).getAndUpdate(newContent); // <- throw
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound() {
        result = batch.blob(alias).getAndUpdate(newContent);
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        QdbInteger integer = Helpers.getInteger(alias);

        result = batch.blob(alias).getAndUpdate(newContent);
        integer.put(666);
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void throwsInvalidArgument_whenExpiryTimeIsInThePast() {
        QdbExpiryTime fewMinutesAgo = QdbExpiryTime.makeMinutesFromNow(-7);

        result = batch.blob(alias).getAndUpdate(newContent, fewMinutesAgo);
        blob.put(originalContent);
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        alias = Helpers.RESERVED_ALIAS;

        result = batch.blob(alias).getAndUpdate(newContent);
        batch.run();
        result.get(); // <- throws
    }

    @Test
    public void returnsOriginalContent() {
        result = batch.blob(alias).getAndUpdate(newContent);
        blob.put(originalContent);
        batch.run();

        Assert.assertEquals(originalContent, result.get());
    }
}
