import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.AliasNotFoundException;
import net.quasardb.qdb.exception.InvalidArgumentException;
import net.quasardb.qdb.exception.ClusterClosedException;
import org.junit.*;

public class QdbClusterEntryTest {
    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        cluster.close();
        cluster.entry(alias); // <- throws
    }

    @Test(expected = AliasNotFoundException.class)
    public void throwsAliasNotFound_whenAliasIsRandom() {
        String alias = Helpers.createUniqueAlias();

        Helpers.getEntry(alias); // <- throws
    }

    @Test(expected = InvalidArgumentException.class)
    public void throwsInvalidArgument_whenAliasIsEmpty() {
        Helpers.getEntry(""); // <- throws
    }

    @Test(expected = InvalidArgumentException.class)
    public void throwsInvalidArgument_whenAliasIsNull() {
        Helpers.getEntry(null); // <- throws
    }

    @Test
    public void returnsQdbBlob_whenEntryIsBlob() {
        QdbBlob blob = Helpers.createBlob();

        String alias = blob.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbBlob);
    }

    @Test
    public void returnsQdbInteger_whenEntryIsInteger() {
        QdbInteger integer = Helpers.createInteger();

        String alias = integer.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbInteger);
    }

    @Test
    public void returnsTag_whenEntryIsTag() throws Exception {
        QdbTag tag = Helpers.createTag();

        String alias = tag.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbTag);
    }
}
