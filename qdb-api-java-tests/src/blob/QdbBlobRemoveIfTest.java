import java.nio.ByteBuffer;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBlobRemoveIfTest {
    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbBlob blob = Helpers.getBlob(alias);
        ByteBuffer comparand = Helpers.createSampleData();

        integer.put(666);
        blob.removeIf(comparand); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        ByteBuffer comparand = Helpers.createSampleData();

        QdbBlob blob = Helpers.getBlob("qdb");
        blob.removeIf(comparand); // <- throws
    }

    @Test
    public void returnsFalse_whenComparandMismatches() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = Helpers.createSampleData();

        blob.put(content);
        boolean result = blob.removeIf(comparand);

        Assert.assertFalse(result);
    }

    @Test
    public void returnsTrue_whenComparandMatches() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer comparand = content.duplicate();

        blob.put(content);
        boolean result = blob.removeIf(comparand);

        Assert.assertTrue(result);
    }
}
