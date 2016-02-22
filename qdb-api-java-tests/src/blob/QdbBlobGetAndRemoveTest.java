import java.nio.ByteBuffer;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBlobGetAndRemoveTest {
    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbBlob blob = Helpers.getBlob(alias);

        integer.put(666);
        blob.getAndRemove(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {

        QdbBlob blob = Helpers.getBlob("qdb");
        blob.getAndRemove(); // <- throws
    }

    @Test
    public void returnsOriginalContent() {
        QdbBlob blob = Helpers.createEmptyBlob();
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        ByteBuffer result = blob.getAndRemove();

        Assert.assertEquals(content, result);
    }
}
