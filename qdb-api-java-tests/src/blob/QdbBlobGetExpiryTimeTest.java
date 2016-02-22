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
        long expiryTimeMillis = System.currentTimeMillis() + 1000 * 60 * 60;
        long expiryTimeSeconds = expiryTimeMillis / 1000;

        blob.put(content);
        blob.expiresAt(new Date(expiryTimeMillis));
        long result = blob.getExpiryTime();

        Assert.assertEquals(expiryTimeSeconds, result);
    }
}
