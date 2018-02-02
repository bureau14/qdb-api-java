import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbBlobGetAndUpdateTest {
    @Test(expected = AliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.getAndUpdate(content); // <- throws
    }

    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        ByteBuffer content = Helpers.createSampleData();
        String alias = Helpers.createUniqueAlias();

        QdbBlob blob = cluster.blob(alias);
        cluster.close();
        blob.getAndUpdate(content); // <- throws
    }

    @Test(expected = IncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbBlob blob = Helpers.getBlob(alias);
        ByteBuffer newContent = Helpers.createSampleData();

        integer.put(666);
        blob.getAndUpdate(newContent); // <- throws
    }

    @Test(expected = InvalidArgumentException.class)
    public void throwsInvalidArgument_whenExpiryTimeIsInThePast() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime fewMinutesAgo = QdbExpiryTime.makeMinutesFromNow(-7);

        blob.put(content);
        blob.getAndUpdate(content, fewMinutesAgo); // <- throws
    }

    @Test(expected = ReservedAliasException.class)
    public void throwsReservedAlias() {
        ByteBuffer newContent = Helpers.createSampleData();

        QdbBlob blob = Helpers.getBlob(Helpers.RESERVED_ALIAS);
        blob.getAndUpdate(newContent); // <- throws
    }

    @Test
    public void returnsOriginalContent() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        blob.put(content1);
        Buffer result = blob.getAndUpdate(content2);

        Assert.assertEquals(content1, result.toByteBuffer());
    }
}
