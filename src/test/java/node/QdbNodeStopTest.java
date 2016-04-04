import net.quasardb.qdb.*;
import org.junit.*;

public class QdbNodeStopTest {

    @Test(expected = QdbOperationDisabledException.class)
    public void throwsOperationDisabled() {
        QdbNode node = Helpers.getNode();

        node.stop("hello world"); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String uri = DaemonRunner.uri();

        QdbNode node = cluster.node(uri);
        cluster.close();
        node.stop("hello world"); // <- throws
    }
}
