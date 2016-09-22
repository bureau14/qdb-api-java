import net.quasardb.qdb.*;
import org.junit.*;

public class QdbClusterNodeTest {
    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String uri = DaemonRunner.uri();

        cluster.close();
        cluster.node(uri); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void throwsInvalidArgument_whenUriIsInvalid() {
        Helpers.getCluster().node("wrong_uri"); // <- throws
    }
}
