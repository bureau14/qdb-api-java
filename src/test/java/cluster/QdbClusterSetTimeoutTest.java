import net.quasardb.qdb.*;
import org.junit.*;

public class QdbClusterSetTimeoutTest {
    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();

        cluster.close();
        cluster.setTimeout(60 * 1000); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void throwsInvalidArgument_whenTimeoutIsLessThanOneSecond() {
        QdbCluster cluster = Helpers.createCluster();

        cluster.setTimeout(999); // <- throws
    }

    @Test
    public void ok_whenTimeoutIsOneSecond() {
        QdbCluster cluster = Helpers.createCluster();

        cluster.setTimeout(1000);
    }
}
