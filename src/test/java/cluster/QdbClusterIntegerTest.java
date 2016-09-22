import net.quasardb.qdb.*;
import org.junit.*;

public class QdbClusterIntegerTest {
    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        cluster.close();
        cluster.integer(alias); // <- throws
    }
}
