import java.nio.ByteBuffer;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBlobGetTest {
    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingCompareAndSwap_thenWaitingExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = content.duplicate();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        blob.compareAndSwap(newContent, comparand, 1);
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingExpiresAt_thenWaitingExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        long expiryTime = System.currentTimeMillis() + 1000;

        blob.put(content);
        blob.expiresAt(new Date(expiryTime));
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingExpiresFromNow_thenWaitingExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        blob.expiresFromNow(0);
        Helpers.wait(1.0);
        blob.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingGetAndRemove() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        blob.getAndRemove();
        blob.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingGetAndUpdate_thenWaitingExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        blob.getAndUpdate(content, 1);
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingPut_thenWaitingExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content, 1);
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingRemove() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        blob.remove();
        blob.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingUpdate_thenWaitingExpiration() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        blob.update(content, 1);
        Helpers.wait(1.5);
        blob.get(); // <- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbBlob blob = Helpers.getBlob(alias);

        integer.put(666);
        blob.get(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        QdbBlob blob = Helpers.getBlob("qdb");
        blob.get(); // <- throws
    }

    @Test
    public void returnsOriginalContent_afterCallingPut() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        ByteBuffer result = blob.get();

        Assert.assertEquals(content, result);
    }

    @Test
    public void returnsOriginalContent_afterCallingCompareAndSwap_whenComparandMismatches() {
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
    public void returnsUpdatedContent_afterCallingGetAndUpdate() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        blob.put(content1);
        blob.getAndUpdate(content2);
        ByteBuffer result = blob.get();

        Assert.assertEquals(content2, result);
    }

    @Test
    public void returnsUpdatedContent_afterCallingUpdate() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        blob.update(newContent);
        ByteBuffer result = blob.get();

        Assert.assertEquals(newContent, result);
    }

    @Test
    public void returnsUpdatedContent_afterCallingCompareAndSwap_whenComparandMatches() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = content.duplicate();
        ByteBuffer newContent = Helpers.createSampleData();

        blob.put(content);
        blob.compareAndSwap(newContent, comparand);
        ByteBuffer result = blob.get();

        Assert.assertEquals(newContent, result);
    }
}
