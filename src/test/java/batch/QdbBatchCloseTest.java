import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBatchCloseTest {
    QdbBatch batch;

    @Before
    public void setUp() {
        batch = Helpers.createBatch();
    }

    @Test
    public void close_doesNotThrow_whenBatchIsEmpty() {
        batch.close();
    }

    @Test
    public void close_doesNotThrow_whenCalledTwice() {
        batch.blob("aaaa").put(Helpers.createSampleData());
        batch.close();
        batch.close();
    }
}
