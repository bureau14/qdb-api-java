import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBlobGetExpiryTimeTest {
    @Test
    public void returnsSameValue_afterCallingPutWithExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime expiry = QdbExpiryTime.makeMinutesFromNow(5);

        blob.put(content, expiry);
        QdbExpiryTime result = blob.expiryTime();

        Assert.assertEquals(expiry, result);
    }

    @Test
    public void returnOriginalValue_afterCallingUpdateWithoutExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime expiry = QdbExpiryTime.makeMinutesFromNow(5);

        blob.put(content, expiry);
        blob.update(content);
        QdbExpiryTime result = blob.expiryTime();

        Assert.assertEquals(expiry, result);
    }

    @Test
    public void returnUpdatedValue_afterCallingUpdateWithExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime expiry = QdbExpiryTime.makeMinutesFromNow(5);

        blob.put(content);
        blob.update(content, expiry);
        QdbExpiryTime result = blob.expiryTime();

        Assert.assertEquals(expiry, result);
    }

    @Test
    public void returnsZero_afterCallingPutWithoutExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        QdbExpiryTime result = blob.expiryTime();

        Assert.assertEquals(QdbExpiryTime.NEVER_EXPIRES, result);
    }
}
