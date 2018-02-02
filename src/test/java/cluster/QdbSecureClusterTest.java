import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbSecureClusterTest {
    @Test
    public void canClose_afterOpen() {
        QdbCluster cluster = Helpers.createSecureCluster();
        String uri = DaemonRunner.secureUri();

        cluster.node(uri); // does not throw
        cluster.close();
    }
}
