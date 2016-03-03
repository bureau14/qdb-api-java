import java.nio.ByteBuffer;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBlobGetExpiryTimeTest {
    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        QdbBlob blob = Helpers.getBlob("qdb");
        blob.getExpiryTime(); // <- throws
    }

    @Test
    public void returnSameValue_afterCallingExpiresAt() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        QdbExpiryTime expiry = QdbExpiryTime.makeMinutesFromNow(1);

        blob.put(content);
        blob.setExpiryTime(expiry);
        QdbExpiryTime result = blob.getExpiryTime();

        Assert.assertEquals(expiry, result);
    }
}
