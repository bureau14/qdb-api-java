import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbEntryHasTagTest {
    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_whenEntryDoesntExists() {
        String tag = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.createEmptyBlob();

        entry.hasTag(tag); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();
        String tag = Helpers.createUniqueAlias();

        QdbEntry entry = cluster.blob(alias);
        cluster.close();
        entry.hasTag(tag); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenAliasIsQdb() {
        String tag = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.getBlob(Helpers.RESERVED_ALIAS);

        entry.hasTag(tag); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenTagIsQdb() {
        QdbEntry entry = Helpers.createEmptyBlob();

        entry.hasTag(Helpers.RESERVED_ALIAS); // <- throws
    }

    @Test
    public void returnsFalse_beforeCallingAttachTag() {
        QdbEntry entry = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        boolean result = entry.hasTag(tag);

        Assert.assertFalse(result);
    }

    @Test
    public void returnsFalse_afterCallingDetachTag() {
        QdbEntry entry = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        entry.attachTag(tag);
        entry.detachTag(tag);
        boolean result = entry.hasTag(tag);

        Assert.assertFalse(result);
    }

    @Test
    public void returnsTrue_afterCallingAttachTag() {
        QdbEntry entry = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        entry.attachTag(tag);
        boolean result = entry.hasTag(tag);

        Assert.assertTrue(result);
    }
}
