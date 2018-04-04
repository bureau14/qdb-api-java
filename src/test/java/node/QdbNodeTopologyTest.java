import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbNodeTopologyTest {
    @Test
    public void returnsJson() {
        QdbNode node = Helpers.getNode();

        String topology = node.topology();

        Assert.assertTrue("This is not JSON: \"" + topology + "\"", Helpers.looksLikeJson(topology));
    }

    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String uri = Daemon.uri();

        QdbNode node = cluster.node(uri);
        cluster.close();
        node.topology(); // <- throws
    }
}
