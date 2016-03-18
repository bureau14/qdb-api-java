import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBatchBlobRemoveIfTest {
    final ByteBuffer originalContent = Helpers.createSampleData();
    final ByteBuffer comparand = Helpers.createSampleData();
    String alias;
    QdbBlob blob;
    QdbBatch batch;
    QdbFuture<Boolean> result;

    @Before
    public void setUp() {
        alias = Helpers.createUniqueAlias();
        blob = Helpers.getBlob(alias);
        batch = Helpers.createBatch();
    }

    @Test(expected = QdbBatchNotRunException.class)
    public void throwsBatchNotRun_beforeCallingRun() {
        result = batch.blob(alias).removeIf(comparand);
        result.get(); // <- throw
    }

    @Test(expected = QdbBatchAlreadyRunException.class)
    public void throwsBatchAlreadyRun_afterCallingRun() {
        batch.run();
        batch.blob(alias).removeIf(comparand); // <- throw
    }

    @Test(expected = QdbBatchClosedException.class)
    public void throwsBatchClosed_afterCallingClose() {
        batch.close();
        batch.blob(alias).removeIf(comparand); // <- throw
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        QdbInteger integer = Helpers.getInteger(alias);

        result = batch.blob(alias).removeIf(comparand);
        integer.put(666);
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        alias = Helpers.RESERVED_ALIAS;

        result = batch.blob(alias).removeIf(comparand);
        batch.run();
        result.get(); // <- throws
    }

    @Test
    public void returnsFalse_whenComparandMismatches() {
        result = batch.blob(alias).removeIf(comparand);
        blob.put(originalContent);
        batch.run();

        Assert.assertFalse(result.get());
    }

    @Test
    public void returnsTrue_whenComparandMatches() {
        result = batch.blob(alias).removeIf(originalContent);
        blob.put(originalContent);
        batch.run();

        Assert.assertTrue(result.get());
    }
}
