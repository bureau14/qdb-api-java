import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbExpirableEntryGetExpiryTimeTest {
    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        QdbExpirableEntry entry = cluster.blob(alias);
        cluster.close();
        entry.expiryTime(); // <- throws
    }

    @Test
    public void returnsSameValue_afterCallingSetExpiry() {
        QdbExpirableEntry entry = Helpers.createBlob();
        QdbExpiryTime expiry = QdbExpiryTime.makeMinutesFromNow(5);

        entry.expiryTime(expiry);
        QdbExpiryTime result = entry.expiryTime();

        Assert.assertEquals(expiry, result);
    }
}
