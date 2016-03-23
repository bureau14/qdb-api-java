import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBatchOperationCountTest {
    QdbBatch batch;

    @Before
    public void setUp() {
        batch = Helpers.createBatch();
    }

    @Test(expected = QdbBatchClosedException.class)
    public void throwsBatchClosed_afterCallingClose() {
        batch.close();
        batch.operationCount(); // <- throw
    }

    @Test
    public void returnsZero_whenBatchIsEmpty() {
        int result = batch.operationCount();

        Assert.assertEquals(0, result);
    }

    @Test
    public void returnsTwo_afterAddingTwoOperations() {
        batch.blob("hello").get();
        batch.blob("world").get();
        int result = batch.operationCount();

        Assert.assertEquals(2, result);
    }
}
