import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbClusterEntryTest {
    @Test(expected = QdbAliasNotFoundException.class)
    public void entry_throwsAliasNotFound_whenAliasIsRandom() {
        String alias = Helpers.createUniqueAlias();

        Helpers.getEntry(alias); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void entry_throwsInvalidArgument_whenAliasIsEmpty() {
        Helpers.getEntry(""); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void entry_throwsInvalidArgument_whenAliasIsNull() {
        Helpers.getEntry(null); // <- throws
    }

    @Test
    public void entry_returnsQdbBlob_whenEntryIsBlob() {
        QdbBlob blob = Helpers.createBlob();

        String alias = blob.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbBlob);
    }

    @Test
    public void entry_returnsQdbDeque_whenEntryIsDeque() {
        QdbDeque deque = Helpers.createDeque();

        String alias = deque.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbDeque);
    }

    @Test
    public void entry_returnsQdbHashSet_whenEntryIsHashSet() {
        QdbHashSet hset = Helpers.createHashSet();

        String alias = hset.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbHashSet);
    }

    @Test
    public void entry_returnsQdbInteger_whenEntryIsInteger() {
        QdbInteger integer = Helpers.createInteger();

        String alias = integer.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbInteger);
    }

    @Test
    public void entry_returnsQdbStream_whenEntryIsStream() throws Exception {
        QdbStream stream = Helpers.createStream();

        String alias = stream.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbStream);
    }

    @Test
    public void entry_returnsQdbTag_whenEntryIsTag() throws Exception {
        QdbTag tag = Helpers.createTag();

        String alias = tag.alias();
        QdbEntry result = Helpers.getEntry(alias);

        Assert.assertTrue(result instanceof QdbTag);
    }
}
