import net.quasardb.qdb.*;
import org.junit.*;

public class QdbClusterTest {
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

    @Test(expected = QdbOperationDisabledException.class)
    public void purgeAll_throwsOperationDisabled() {
        Helpers.getCluster().purgeAll(); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void node_throwsInvalidArgument_whenUriIsInvalid() {
        Helpers.getCluster().node("wrong_uri"); // <- throws
    }

    @Test
    public void version_returnNonEmptyString() {
        String version = QdbCluster.version();
        Assert.assertTrue(version.length() > 5);
    }

    @Test
    public void build_returnNonEmptyString() {
        String build = QdbCluster.build();
        Assert.assertTrue(build.length() > 5);
    }
}
