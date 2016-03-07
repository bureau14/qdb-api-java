import java.nio.ByteBuffer;
import java.util.Date;
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

        QdbBlob blob = Helpers.getBlob("qdb");
        blob.put(content); // <- throws
    }
}
