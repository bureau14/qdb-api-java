import net.quasardb.qdb.*;
import org.junit.*;

public class QdbClusterTest {
    @Test
    public void build_returnsNonEmptyString() {
        String build = QdbCluster.build();
        Assert.assertTrue(build.length() > 5);
    }

    @Test(expected = QdbClusterClosedException.class)
    public void blob_throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        cluster.close();
        cluster.blob(alias); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void createBatch_throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();

        cluster.close();
        cluster.createBatch(); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void constructor_throwsInvalidArgument_whenUriIsInvalid() {
        new QdbCluster("wrong_uri");
    }

    @Test(expected = QdbConnectionRefusedException.class)
    public void constructor_throwsInvalidArgument_whenUriPointsToNothing() {
        new QdbCluster("qdb://127.0.0.1:1");
    }

    @Test(expected = QdbHostNotFoundException.class)
    public void constructor_throwsHostNotFound_whenUriContainsANonExistingName() {
        new QdbCluster("qdb://helloworld:666");
    }

    @Test(expected = QdbClusterClosedException.class)
    public void deque_throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        cluster.close();
        cluster.deque(alias); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void entry_throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        cluster.close();
        cluster.entry(alias); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void findNodeFor_throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        cluster.close();
        cluster.findNodeFor(alias); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void hashSet_throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        cluster.close();
        cluster.hashSet(alias); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void integer_throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        cluster.close();
        cluster.integer(alias); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void node_throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String uri = DaemonRunner.uri();

        cluster.close();
        cluster.node(uri); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void node_throwsInvalidArgument_whenUriIsInvalid() {
        Helpers.getCluster().node("wrong_uri"); // <- throws
    }

    @Test(expected = QdbOperationDisabledException.class)
    public void purgeAll_throwsOperationDisabled() {
        QdbCluster cluster = Helpers.createCluster();

        cluster.purgeAll(); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void purgeAll_throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();

        cluster.close();
        cluster.purgeAll(); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void stream_throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        cluster.close();
        cluster.stream(alias); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void tag_throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        cluster.close();
        cluster.tag(alias); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void trimAll_throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();

        cluster.close();
        cluster.trimAll(); // <- throws
    }

    public void version_returnsNonEmptyString() {
        String version = QdbCluster.version();
        Assert.assertTrue(version.length() > 5);
    }
}
