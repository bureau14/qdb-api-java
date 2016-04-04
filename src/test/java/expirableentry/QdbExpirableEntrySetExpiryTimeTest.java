import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbExpirableEntrySetExpiryTimeTest {
    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_whenEntryDoesntExists() {
        QdbExpirableEntry entry = Helpers.createEmptyBlob();
        QdbExpiryTime expiry = QdbExpiryTime.makeSecondsFromNow(42);

        entry.expiryTime(expiry); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();
        QdbExpiryTime expiry = QdbExpiryTime.makeSecondsFromNow(42);

        QdbExpirableEntry entry = cluster.blob(alias);
        cluster.close();
        entry.expiryTime(expiry); // <- throws
    }
}
