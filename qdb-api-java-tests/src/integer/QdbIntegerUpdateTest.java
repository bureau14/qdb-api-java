import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbIntegerUpdateTest {
    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwIncompatibleType() {
        String alias = Helpers.createUniqueAlias();
        QdbBlob blob = Helpers.getBlob(alias);
        QdbInteger integer = Helpers.getInteger(alias);
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        integer.set(666); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwReservedAlias() {
        QdbInteger integer = Helpers.getInteger("qdb");
        integer.set(666); // <- throws
    }
}
