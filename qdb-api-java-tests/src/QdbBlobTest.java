import java.nio.ByteBuffer;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBlobTest {
    @Test
    public void get_returnsOriginalContent_afterCallingPut() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        ByteBuffer result = blob.get();

        Assert.assertEquals(content, result);
    }

    @Test
    public void get_returnsOriginalContent_afterCallingCompareAndSwap_whenComparandMismatches() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        blob.compareAndSwap(newContent, comparand);
        ByteBuffer result = blob.get();

        Assert.assertEquals(content, result);
    }

    @Test
    public void get_returnsUpdatedContent_afterCallingGetAndUpdate() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        blob.put(content1);
        blob.getAndUpdate(content2);
        ByteBuffer result = blob.get();

        Assert.assertEquals(content2, result);
    }

    @Test
    public void get_returnsUpdatedContent_afterCallingUpdate() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        blob.update(newContent);
        ByteBuffer result = blob.get();

        Assert.assertEquals(newContent, result);
    }

    @Test
    public void get_returnsUpdatedContent_afterCallingCompareAndSwap_whenComparandMatches() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = content.duplicate();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        blob.compareAndSwap(newContent, comparand);
        ByteBuffer result = blob.get();

        Assert.assertEquals(newContent, result);
    }

    @Test(expected = QdbException.class)
    public void get_throws_afterCallingCompareAndSwap_thenWaitingExpiration() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = content.duplicate();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        blob.compareAndSwap(newContent, comparand, 1);
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbException.class)
    public void get_throws_afterCallingExpiresAt_thenWaitingExpiration() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        long expiryTime = System.currentTimeMillis() + 1000;

        blob.put(content);
        blob.expiresAt(new Date(expiryTime));
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbException.class)
    public void get_throws_afterCallingExpiresFromNow_thenWaitingExpiration() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        blob.expiresFromNow(0);
        Helpers.wait(1.0);
        blob.get(); // <- throws
    }

    @Test(expected = QdbException.class)
    public void get_throws_afterCallingGetAndRemove() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        blob.getAndRemove();
        blob.get(); // <- throws
    }

    @Test(expected = QdbException.class)
    public void get_throws_afterCallingGetAndUpdate_thenWaitingExpiration() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        blob.getAndUpdate(content, 1);
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbException.class)
    public void get_throws_afterCallingPut_thenWaitingExpiration() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content, 1);
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbException.class)
    public void get_throws_afterCallingRemove() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        blob.remove();
        blob.get(); // <- throws
    }

    @Test(expected = QdbException.class)
    public void get_throws_afterCallingUpdate_thenWaitingExpiration() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        blob.update(content, 1);
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test
    public void getAlias_returnsSameAlias() throws QdbException {
        String alias = Helpers.createUniqueAlias();
        QdbBlob blob = Helpers.getBlob(alias);

        String result = blob.getAlias();

        Assert.assertEquals(alias, result);
    }

    @Test()
    public void getAndRemove_returnsOriginalContent() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        ByteBuffer result = blob.getAndRemove();

        Assert.assertEquals(content, result);
    }

    @Test
    public void getAndUpdate_returnsOriginalContent() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        blob.put(content1);
        ByteBuffer result = blob.getAndUpdate(content2);

        Assert.assertEquals(content1, result);
    }

    @Test
    public void getExpiryTime_returnSameValue_afterCallingExpiresAt() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        long expiryTimeMillis = System.currentTimeMillis() + 1000 * 60 * 60;
        long expiryTimeSeconds = expiryTimeMillis / 1000;

        blob.put(content);
        blob.expiresAt(new Date(expiryTimeMillis));
        long result = blob.getExpiryTime();

        Assert.assertEquals(expiryTimeSeconds, result);
    }

    @Test
    public void compareAndSwap_returnsNull_whenComparandMatches() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = content.duplicate();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        ByteBuffer result = blob.compareAndSwap(newContent, comparand);

        Assert.assertNull(result);
    }

    @Test
    public void compareAndSwap_returnsOriginalContent_whenComparandMismatches() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        ByteBuffer result = blob.compareAndSwap(newContent, comparand);

        Assert.assertEquals(content, result);
    }

    @Test
    public void removeIf_returnsFalse_whenComparandMismatches() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = Helpers.createSampleData();

        blob.put(content);
        boolean result = blob.removeIf(comparand);

        Assert.assertFalse(result);
    }

    @Test
    public void removeIf_returnsTrue_whenComparandMatches() throws QdbException {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = content.duplicate();

        blob.put(content);
        boolean result = blob.removeIf(comparand);

        Assert.assertTrue(result);
    }
}
