import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbEntryRemoveTest {
    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_ifCalledTwice() {
        QdbEntry entry = Helpers.createBlob();

        entry.remove();
        entry.remove(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenAliasIsQdb() {
        QdbEntry entry = Helpers.getBlob("qdb");

        entry.remove(); // <- throws
    }
}
