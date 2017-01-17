import java.time.Instant;
import java.util.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;
import org.junit.*;

public class QdbEntryMetadataTest {
    @Test
    public void returnsMetadata() {
        QdbBlob blob = Helpers.createEmptyBlob();
        java.nio.ByteBuffer data = Helpers.createSampleData();
        // Instant beforePut = Instant.now();
        blob.put(data);
        // Instant afterPut = Instant.now();

        QdbEntryMetadata meta = blob.metadata();
        long[] id = meta.reference().data();

        Assert.assertNotEquals(0, id[0]);
        Assert.assertNotEquals(0, id[1]);
        Assert.assertNotEquals(0, id[2]);
        Assert.assertNotEquals(0, id[3]);

        Assert.assertEquals(data.limit(), meta.size());

        Assert.assertNotEquals(Instant.ofEpochSecond(0), meta.lastModificationTime());
        Assert.assertEquals(Instant.ofEpochSecond(0), meta.expiryTime());

        // Assert.assertTrue(beforePut.isBefore(meta.lastModificationTime()));
        // Assert.assertTrue(afterPut.isAfter(meta.lastModificationTime()));
    }

    @Test
    public void returnsMetadata_afterSettingExpiryTime() {
        QdbBlob blob = Helpers.createEmptyBlob();
        java.nio.ByteBuffer data = Helpers.createSampleData();
        QdbExpiryTime expiry = QdbExpiryTime.makeMinutesFromNow(5);
        Instant expiryInstant = Instant.ofEpochMilli(expiry.toMillisSinceEpoch());
        blob.put(data, expiry);

        QdbEntryMetadata meta = blob.metadata();
        Assert.assertNotEquals(Instant.ofEpochSecond(0), meta.lastModificationTime());
        Assert.assertEquals(expiryInstant, meta.expiryTime());
    }
}
