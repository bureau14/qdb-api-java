import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbClusterTrimAllTest {
    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();

        cluster.close();
        cluster.trimAll(60); // <- throws
    }
}
