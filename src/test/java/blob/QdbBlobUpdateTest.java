import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbBlobUpdateTest {
    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();
        ByteBuffer newContent = Helpers.createSampleData();

        QdbBlob blob = cluster.blob(alias);
        cluster.close();
        blob.update(newContent); // <- throws
    }

    @Test(expected = IncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbBlob blob = Helpers.getBlob(alias);
        ByteBuffer newContent = Helpers.createSampleData();

        integer.put(666);
        blob.update(newContent); // <- throws
    }

    @Test(expected = InvalidArgumentException.class)
    public void throwsInvalidArgument_whenExpiryTimeIsInThePast() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime fewMinutesAgo = QdbExpiryTime.makeMinutesFromNow(-7);

        blob.update(content, fewMinutesAgo); // <- throws
    }

    @Test(expected = ReservedAliasException.class)
    public void throwsReservedAlias() {
        ByteBuffer newContent = Helpers.createSampleData();

        QdbBlob blob = Helpers.getBlob(Helpers.RESERVED_ALIAS);
        blob.update(newContent); // <- throws
    }

    @Test
    public void returnsTrue_whenCalledOnce() {
        ByteBuffer newContent = Helpers.createSampleData();
        QdbBlob blob = Helpers.createEmptyBlob();

        boolean created = blob.update(newContent);

        Assert.assertTrue(created);
    }

    @Test
    public void returnsFalse_whenCalledOnce() {
        ByteBuffer newContent = Helpers.createSampleData();
        QdbBlob blob = Helpers.createEmptyBlob();

        blob.update(newContent);
        boolean created = blob.update(newContent);

        Assert.assertFalse(created);
    }
}
