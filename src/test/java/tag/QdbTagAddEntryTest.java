import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbTagAddEntryTest {
    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_whenAliasDoesntExists() {
        QdbTag tag = Helpers.createEmptyTag();
        String alias = Helpers.createUniqueAlias();

        tag.addEntry(alias); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_whenEntryDoesntExists() {
        QdbTag tag = Helpers.createEmptyTag();
        QdbEntry entry = Helpers.createEmptyBlob();

        tag.addEntry(entry); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.createEmptyBlob();

        QdbTag tag = cluster.tag(alias);
        cluster.close();
        tag.addEntry(entry); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenAliasIsQdb() {
        QdbTag tag = Helpers.createEmptyTag();
        String alias = Helpers.RESERVED_ALIAS;

        tag.addEntry(alias); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenTagIsQdb() {
        QdbTag tag = Helpers.getTag(Helpers.RESERVED_ALIAS);
        QdbEntry entry = Helpers.createEmptyBlob();

        tag.addEntry(entry); // <- throws
    }

    @Test
    public void returnsTrue_whenCalledOnce() {
        QdbEntry entry = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        boolean result = tag.addEntry(entry);

        Assert.assertTrue(result);
    }

    @Test
    public void returnsFalse_whenCalledTwice() {
        QdbEntry entry = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        tag.addEntry(entry);
        boolean result = tag.addEntry(entry);

        Assert.assertFalse(result);
    }
}
