import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbNodeStatusTest {
    @Test
    public void returnsJson() {
        QdbNode node = Helpers.getNode();

        String status = node.status();

        Assert.assertTrue("This is not JSON: \"" + status + "\"", Helpers.looksLikeJson(status));
    }

    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String uri = Daemon.uri();

        QdbNode node = cluster.node(uri);
        cluster.close();
        node.status(); // <- throws
    }
}
