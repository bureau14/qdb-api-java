import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import org.junit.*;

import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;

public class QdbHashSetContainsTest {
    @Test(expected = AliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbHashSet hset = Helpers.createEmptyHashSet();
        ByteBuffer content = Helpers.createSampleData();

        hset.contains(content); // <- throws
    }

    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        ByteBuffer content = Helpers.createSampleData();
        String alias = Helpers.createUniqueAlias();

        QdbHashSet hset = cluster.hashSet(alias);
        cluster.close();
        hset.contains(content); // <- throws
    }

    @Test(expected = IncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingBlobPut() {
        String alias = Helpers.createUniqueAlias();
        QdbBlob blob = Helpers.getBlob(alias);
        QdbHashSet hset = Helpers.getHashSet(alias);
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        hset.contains(content); // <- throws
    }

    @Test(expected = ReservedAliasException.class)
    public void throwsReservedAlias() {
        ByteBuffer content = Helpers.createSampleData();

        QdbHashSet hset = Helpers.getHashSet(Helpers.RESERVED_ALIAS);
        hset.contains(content); // <- throws
    }

    @Test
    public void returnsTrue_afterCallingInsert() {
        QdbHashSet hset = Helpers.createEmptyHashSet();
        ByteBuffer content = Helpers.createSampleData();

        hset.insert(content);
        boolean result = hset.contains(content);

        Assert.assertTrue(result);
    }

    @Test
    public void returnsFalse_afterCallingErase() {
        QdbHashSet hset = Helpers.createEmptyHashSet();
        ByteBuffer content = Helpers.createSampleData();

        hset.insert(content);
        hset.erase(content);
        boolean result = hset.contains(content);

        Assert.assertFalse(result);
    }
}
