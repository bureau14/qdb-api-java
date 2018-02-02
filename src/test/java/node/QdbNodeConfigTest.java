import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbNodeConfigTest {
    @Test
    public void returnsJson() {
        QdbNode node = Helpers.getNode();

        String config = node.config();

        Assert.assertTrue("This is not JSON: \"" + config + "\"", Helpers.looksLikeJson(config));
    }

    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String uri = DaemonRunner.uri();

        QdbNode node = cluster.node(uri);
        cluster.close();
        node.config(); // <- throws
    }
}
