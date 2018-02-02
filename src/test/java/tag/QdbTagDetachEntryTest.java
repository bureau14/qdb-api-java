import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbTagDetachEntryTest {
    @Test(expected = AliasNotFoundException.class)
    public void throwsAliasNotFound_whenAliasDoesntExists() {
        QdbTag tag = Helpers.createTag();
        String entry = Helpers.createUniqueAlias();

        tag.detachEntry(entry); // <- throws
    }

    @Test(expected = AliasNotFoundException.class)
    public void throwsAliasNotFound_whenEntryDoesntExists() {
        QdbTag tag = Helpers.createTag();
        QdbEntry entry = Helpers.createEmptyBlob();

        tag.detachEntry(entry); // <- throws
    }

    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.createEmptyBlob();

        QdbTag tag = cluster.tag(alias);
        cluster.close();
        tag.detachEntry(entry); // <- throws
    }

    @Test(expected = ReservedAliasException.class)
    public void throwsReservedAlias_whenAliasIsQdb() {
        QdbTag tag = Helpers.getTag(Helpers.RESERVED_ALIAS);

        tag.detachEntry("toto"); // <- throws
    }

    @Test(expected = ReservedAliasException.class)
    public void throwsReservedAlias_whenTagIsQdb() {
        QdbTag tag = Helpers.createTag();

        tag.detachEntry(Helpers.RESERVED_ALIAS); // <- throws
    }

    @Test
    public void returnsTrue_whenCalledOnce() {
        QdbEntry entry = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        tag.attachEntry(entry);
        boolean result = tag.detachEntry(entry);

        Assert.assertTrue(result);
    }

    @Test
    public void returnsFalse_whenCalledTwice() {
        QdbEntry entry = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        tag.attachEntry(entry);
        tag.detachEntry(entry);
        boolean result = tag.detachEntry(entry);

        Assert.assertFalse(result);
    }
}
