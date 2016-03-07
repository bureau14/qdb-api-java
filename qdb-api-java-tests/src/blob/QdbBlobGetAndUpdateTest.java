import java.nio.ByteBuffer;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBlobGetAndUpdateTest {
    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbBlob blob = Helpers.getBlob(alias);
        ByteBuffer newContent = Helpers.createSampleData();

        integer.put(666);
        blob.getAndUpdate(newContent); // <- throws
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void throwsInvalidArgument_whenExpiryTimeIsInThePast() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime fiveMinutesAgo = QdbExpiryTime.makeMinutesFromNow(-5);

        blob.put(content);
        blob.getAndUpdate(content, fiveMinutesAgo); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        ByteBuffer newContent = Helpers.createSampleData();

        QdbBlob blob = Helpers.getBlob("qdb");
        blob.getAndUpdate(newContent); // <- throws
    }

    @Test
    public void returnsOriginalContent() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        blob.put(content1);
        ByteBuffer result = blob.getAndUpdate(content2);

        Assert.assertEquals(content1, result);
    }
}
