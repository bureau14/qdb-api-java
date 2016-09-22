import net.quasardb.qdb.*;
import org.junit.*;

public class QdbClusterTrimAllTest {
    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();

        cluster.close();
        cluster.trimAll(60); // <- throws
    }
}
