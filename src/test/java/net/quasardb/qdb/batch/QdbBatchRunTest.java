import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbBatchRunTest {
    QdbBatch batch;

    @Before
    public void setUp() {
        batch = Helpers.createBatch();
    }

    @Test(expected = BatchAlreadyRunException.class)
    public void throwsBatchAlreadyRun_whenCalledTwice() {
        batch.run();
        batch.run();
    }

    @Test
    public void doesNotThrow_whenBatchIsEmpty() {
        batch.run();
    }

    @Test(expected = BatchClosedException.class)
    public void throwsBatchClosed_afterCallingClose() {
        batch.close();
        batch.run(); // <- throw
    }
}
