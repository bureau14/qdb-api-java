import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbClusterFindNodeForTest {
    @Test
    public void returnsAddressAndPort() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        QdbNode node = cluster.findNodeFor(alias);
        Assert.assertEquals("127.0.0.1", node.hostName());
        Assert.assertEquals(DaemonRunner.port(), node.port());
    }

    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        cluster.close();
        cluster.findNodeFor(alias); // <- throws
    }
}
