import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbBlobGetAndRemoveTest {
    @Test(expected = AliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbBlob blob = Helpers.createEmptyBlob();

        blob.getAndRemove(); // <- throws
    }

    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        QdbBlob blob = cluster.blob(alias);
        cluster.close();
        blob.getAndRemove(); // <- throws
    }

    @Test(expected = IncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbBlob blob = Helpers.getBlob(alias);

        integer.put(666);
        blob.getAndRemove(); // <- throws
    }

    @Test(expected = ReservedAliasException.class)
    public void throwsReservedAlias() {
        String alias = Helpers.RESERVED_ALIAS;

        QdbBlob blob = Helpers.getBlob(alias);
        blob.getAndRemove(); // <- throws
    }

    @Test
    public void returnsOriginalContent() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        Buffer result = blob.getAndRemove();

        Assert.assertEquals(content, result.toByteBuffer());
    }
}
