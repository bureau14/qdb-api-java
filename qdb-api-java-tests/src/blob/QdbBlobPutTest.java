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

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        ByteBuffer content = Helpers.createSampleData();

        QdbBlob blob = Helpers.getBlob("qdb");
        blob.put(content); // <- throws
    }
}
