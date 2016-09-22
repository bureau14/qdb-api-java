import net.quasardb.qdb.*;
import org.junit.*;

public class QdbClusterVersionTest {
    @Test
    public void build_returnsNonEmptyString() {
        String build = QdbCluster.build();
        Assert.assertTrue(build.length() > 5);
    }

    @Test
    public void version_returnsNonEmptyString() {
        String version = QdbCluster.version();
        Assert.assertTrue(version.length() > 5);
    }
}
