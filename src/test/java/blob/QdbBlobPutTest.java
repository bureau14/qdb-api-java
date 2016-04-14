import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBlobPutTest {
    @Test(expected = QdbAliasAlreadyExistsException.class)
    public void throwsAliasAlreadyExists_whenCalledTwice() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        blob.put(content); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        ByteBuffer content = Helpers.createSampleData();
        String alias = Helpers.createUniqueAlias();

        QdbBlob blob = cluster.blob(alias);
        cluster.close();
        blob.put(content); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void throwsInvalidArgument_whenExpiryTimeIsInThePast() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime fiveMinutesAgo = QdbExpiryTime.makeMinutesFromNow(-5);

        blob.put(content, fiveMinutesAgo); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        ByteBuffer content = Helpers.createSampleData();

        QdbBlob blob = Helpers.getBlob(Helpers.RESERVED_ALIAS);
        blob.put(content); // <- throws
    }
}
