import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbTagRemoveEntryTest {
    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_whenAliasDoesntExists() {
        QdbTag tag = Helpers.createTag();
        String entry = Helpers.createUniqueAlias();

        tag.removeEntry(entry); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_whenEntryDoesntExists() {
        QdbTag tag = Helpers.createTag();
        QdbEntry entry = Helpers.createEmptyBlob();

        tag.removeEntry(entry); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.createEmptyBlob();

        QdbTag tag = cluster.tag(alias);
        cluster.close();
        tag.removeEntry(entry); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenAliasIsQdb() {
        QdbTag tag = Helpers.getTag(Helpers.RESERVED_ALIAS);

        tag.removeEntry("toto"); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenTagIsQdb() {
        QdbTag tag = Helpers.createTag();

        tag.removeEntry(Helpers.RESERVED_ALIAS); // <- throws
    }

    @Test
    public void returnsTrue_whenCalledOnce() {
        QdbEntry entry = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        tag.addEntry(entry);
        boolean result = tag.removeEntry(entry);

        Assert.assertTrue(result);
    }

    @Test
    public void returnsFalse_whenCalledTwice() {
        QdbEntry entry = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        tag.addEntry(entry);
        tag.removeEntry(entry);
        boolean result = tag.removeEntry(entry);

        Assert.assertFalse(result);
    }
}
