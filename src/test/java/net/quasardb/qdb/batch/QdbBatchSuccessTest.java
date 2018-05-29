import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import org.junit.*;

import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;

public class QdbBatchSuccessTest {
    QdbBatch batch;

    @Before
    public void setUp() {
        batch = Helpers.createBatch();
    }

    @Test(expected = BatchClosedException.class)
    public void throwsBatchClosed_afterCallingClose() {
        batch.close();
        batch.success(); // <- throw
    }

    @Test(expected = BatchNotRunException.class)
    public void throwsBatchNotRun_beforeCallingRun() {
        batch.success(); // <- throw
    }

    @Test
    public void returnTrue_whenBatchIsEmpty() {
        batch.run();
        boolean result = batch.success();

        Assert.assertTrue(result);
    }

    @Test
    public void returnTrue_whenAllOperationsSucceed() {
        String alias1 = Helpers.createUniqueAlias();
        String alias2 = Helpers.createUniqueAlias();
        ByteBuffer content = Helpers.createSampleData();

        batch.blob(alias1).put(content);
        batch.blob(alias2).put(content);
        batch.run();
        boolean result = batch.success();

        Assert.assertTrue(result);
    }

    @Test
    public void returnTrue_whenOneOperationFails() {
        String alias1 = Helpers.createUniqueAlias();
        String alias2 = Helpers.createUniqueAlias();
        ByteBuffer content = Helpers.createSampleData();

        batch.blob(alias1).put(content);
        batch.blob(alias2).get();
        batch.run();
        boolean result = batch.success();

        Assert.assertFalse(result);
    }
}
