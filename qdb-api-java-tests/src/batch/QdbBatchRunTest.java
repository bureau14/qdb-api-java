import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBatchRunTest {
    QdbBatch batch;

    @Before
    public void setUp() {
        batch = Helpers.createBatch();
    }

    @Test(expected = QdbBatchAlreadyRunException.class)
    public void throwsBatchAlreadyRun_whenCalledTwice() {
        batch.run();
        batch.run();
    }

    @Test
    public void doesNotThrow_whenBatchIsEmpty() {
        batch.run();
    }

    @Test(expected = QdbBatchClosedException.class)
    public void throwsBatchClosed_afterCallingClose() {
        batch.close();
        batch.run(); // <- throw
    }
}
