import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbBatchBlobPutTest {
    final ByteBuffer content = Helpers.createSampleData();
    String alias;
    QdbBlob blob;
    QdbBatch batch;
    QdbFuture<Void> result;

    @Before
    public void setUp() {
        alias = Helpers.createUniqueAlias();
        blob = Helpers.getBlob(alias);
        batch = Helpers.createBatch();
    }

    @Test(expected = BatchNotRunException.class)
    public void throwsBatchNotRun_beforeCallingRun() {
        result = batch.blob(alias).put(content);
        result.get(); // <- throw
    }

    @Test(expected = BatchAlreadyRunException.class)
    public void throwsBatchAlreadyRun_afterCallingRun() {
        batch.run();
        batch.blob(alias).put(content); // <- throw
    }

    @Test(expected = BatchClosedException.class)
    public void throwsBatchClosed_afterCallingClose() {
        batch.close();
        batch.blob(alias).put(content); // <- throw
    }

    @Test(expected = AliasAlreadyExistsException.class)
    public void throwsAliasAlreadyExists_afterCallingPut() {
        result = batch.blob(alias).put(content);
        blob.put(content);
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = InvalidArgumentException.class)
    public void throwsInvalidArgument_whenExpiryTimeIsInThePast() {
        QdbExpiryTime fewMinutesAgo = QdbExpiryTime.makeMinutesFromNow(-7);

        result = batch.blob(alias).put(content, fewMinutesAgo);
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = ReservedAliasException.class)
    public void throwsReservedAlias() {
        alias = Helpers.RESERVED_ALIAS;

        result = batch.blob(alias).put(content);
        batch.run();
        result.get(); // <- throws
    }
}
