import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBatchBlobUpdateTest {
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
        result = batch.blob(alias).update(content);
        result.get(); // <- throw
    }

    @Test(expected = BatchAlreadyRunException.class)
    public void throwsBatchAlreadyRun_afterCallingRun() {
        batch.run();
        batch.blob(alias).update(content); // <- throw
    }

    @Test(expected = BatchClosedException.class)
    public void throwsBatchClosed_afterCallingClose() {
        batch.close();
        batch.blob(alias).update(content); // <- throw
    }

    @Test(expected = IncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        QdbInteger integer = Helpers.getInteger(alias);

        result = batch.blob(alias).update(content);
        integer.put(666);
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = InvalidArgumentException.class)
    public void throwsInvalidArgument_whenExpiryTimeIsInThePast() {
        QdbExpiryTime fewMinutesAgo = QdbExpiryTime.makeMinutesFromNow(-7);

        result = batch.blob(alias).update(content, fewMinutesAgo);
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = ReservedAliasException.class)
    public void throwsReservedAlias() {
        alias = Helpers.RESERVED_ALIAS;

        result = batch.blob(alias).update(content);
        batch.run();
        result.get(); // <- throws
    }
}
