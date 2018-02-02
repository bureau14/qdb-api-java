import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbClusterNodeTest {
    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String uri = DaemonRunner.uri();

        cluster.close();
        cluster.node(uri); // <- throws
    }

    @Test(expected = InvalidArgumentException.class)
    public void throwsInvalidArgument_whenUriIsInvalid() {
        Helpers.getCluster().node("wrong_uri"); // <- throws
    }
}
