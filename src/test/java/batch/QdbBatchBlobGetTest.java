import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbBatchBlobGetTest {
    final ByteBuffer content = Helpers.createSampleData();
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

    @Test(expected = BatchNotRunException.class)
    public void throwsBatchNotRun_beforeCallingRun() {
        result = batch.blob(alias).get();
        result.get(); // <- throw
    }

    @Test(expected = BatchAlreadyRunException.class)
    public void throwsBatchAlreadyRun_afterCallingRun() {
        batch.run();
        batch.blob(alias).get(); // <- throw
    }

    @Test(expected = BatchClosedException.class)
    public void throwsBatchClosed_afterCallingClose() {
        batch.close();
        batch.blob(alias).get(); // <- throw
    }

    @Test(expected = AliasNotFoundException.class)
    public void throwsAliasNotFound() {
        result = batch.blob(alias).get();
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = IncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        QdbInteger integer = Helpers.getInteger(alias);

        result = batch.blob(alias).get();
        integer.put(666);
        batch.run();
        result.get(); // <- throws
    }

    @Test
    public void returnsOriginalContent_afterCallingPut() {
        result = batch.blob(alias).get();
        blob.put(content);
        batch.run();

        Assert.assertEquals(content, result.get());
    }
}
