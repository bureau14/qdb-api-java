import net.quasardb.qdb.*;
import org.junit.*;

public class QdbNodeTopologyTest {
    @Test
    public void returnsJson() {
        QdbNode node = Helpers.getNode();

        String topology = node.topology();

        Assert.assertTrue("This is not JSON: \"" + topology + "\"", Helpers.looksLikeJson(topology));
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String uri = DaemonRunner.uri();

        QdbNode node = cluster.node(uri);
        cluster.close();
        node.topology(); // <- throws
    }
}
