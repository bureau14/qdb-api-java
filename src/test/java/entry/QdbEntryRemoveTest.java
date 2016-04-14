import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbEntryRemoveTest {
    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_ifCalledTwice() {
        QdbEntry entry = Helpers.createBlob();

        entry.remove();
        entry.remove(); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        QdbEntry entry = cluster.blob(alias);
        cluster.close();
        entry.remove(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenAliasIsQdb() {
        QdbEntry entry = Helpers.getBlob(Helpers.RESERVED_ALIAS);

        entry.remove(); // <- throws
    }
}
