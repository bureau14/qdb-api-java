import net.quasardb.qdb.*;
import org.junit.*;
import java.nio.ByteBuffer;

public class QdbBatchSuccessCountTest {
    QdbBatch batch;

    @Before
    public void setUp() {
        batch = Helpers.createBatch();
    }

    @Test(expected = QdbBatchNotRunException.class)
    public void throwsBatchNotRun_beforeCallingRun() {
        batch.successCount(); // <- throw
    }

    @Test(expected = QdbBatchClosedException.class)
    public void throwsBatchClosed_afterCallingClose() {
        batch.close();
        batch.successCount(); // <- throw
    }

    @Test
    public void returnsOne_afterRunningTwoOperations() {
        String alias1 = Helpers.createUniqueAlias();
        String alias2 = Helpers.createUniqueAlias();
        ByteBuffer content = Helpers.createSampleData();

        batch.blob(alias1).put(content);
        batch.blob(alias2).get();
        batch.run();
        int result = batch.successCount();

        Assert.assertEquals(1, result);
    }
}
