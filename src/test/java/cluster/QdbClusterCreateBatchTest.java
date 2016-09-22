import net.quasardb.qdb.*;
import org.junit.*;

public class QdbClusterCreateBatchTest {
    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();

        cluster.close();
        cluster.createBatch(); // <- throws
    }
}
