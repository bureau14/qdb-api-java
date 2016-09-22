import net.quasardb.qdb.*;
import org.junit.*;

public class QdbClusterPurgeAllTest {
    @Test(expected = QdbOperationDisabledException.class)
    public void throwsOperationDisabled() {
        QdbCluster cluster = Helpers.createCluster();

        cluster.purgeAll(60); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();

        cluster.close();
        cluster.purgeAll(60); // <- throws
    }
}
