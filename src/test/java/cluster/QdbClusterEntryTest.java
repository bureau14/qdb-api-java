import net.quasardb.qdb.*;
import org.junit.*;

public class QdbClusterEntryTest {
    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        cluster.close();
        cluster.entry(alias); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_whenAliasIsRandom() {
        String alias = Helpers.createUniqueAlias();

        Helpers.getEntry(alias); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void throwsInvalidArgument_whenAliasIsEmpty() {
        Helpers.getEntry(""); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
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
    public void returnsQdbDeque_whenEntryIsDeque() {
        QdbDeque deque = Helpers.createDeque();

        String alias = deque.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbDeque);
    }

    @Test
    public void returnsQdbHashSet_whenEntryIsHashSet() {
        QdbHashSet hset = Helpers.createHashSet();

        String alias = hset.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbHashSet);
    }

    @Test
    public void returnsQdbInteger_whenEntryIsInteger() {
        QdbInteger integer = Helpers.createInteger();

        String alias = integer.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbInteger);
    }

    @Test
    public void returnsQdbStream_whenEntryIsStream() throws Exception {
        QdbStream stream = Helpers.createStream();

        String alias = stream.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbStream);
    }

    @Test
    public void returnsQdbTag_whenEntryIsTag() throws Exception {
        QdbTag tag = Helpers.createTag();

        String alias = tag.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbTag);
    }
}
