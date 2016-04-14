import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbHashSetInsertTest {
    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleType() {
        String alias = Helpers.createUniqueAlias();
        QdbBlob blob = Helpers.getBlob(alias);
        QdbHashSet hset = Helpers.getHashSet(alias);
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        hset.insert(content); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        ByteBuffer content = Helpers.createSampleData();
        String alias = Helpers.createUniqueAlias();

        QdbHashSet hset = cluster.hashSet(alias);
        cluster.close();
        hset.insert(content); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        ByteBuffer content = Helpers.createSampleData();

        QdbHashSet hset = Helpers.getHashSet(Helpers.RESERVED_ALIAS);
        hset.insert(content); // <- throws
    }

    @Test
    public void returnsTrue() {
        QdbHashSet hset = Helpers.createEmptyHashSet();
        ByteBuffer content = Helpers.createSampleData();

        boolean result = hset.insert(content);

        Assert.assertTrue(result);
    }

    @Test
    public void returnsFalse_whenCalledTwice() {
        QdbHashSet hset = Helpers.createEmptyHashSet();
        ByteBuffer content = Helpers.createSampleData();

        hset.insert(content);
        boolean result = hset.insert(content);

        Assert.assertFalse(hset.insert(content));
    }
}
