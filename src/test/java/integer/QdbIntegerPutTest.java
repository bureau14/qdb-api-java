import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbIntegerPutTest {
    @Test(expected = QdbAliasAlreadyExistsException.class)
    public void throwAliasAlreadyExists_whenCalledTwice() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(42);
        integer.put(666); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        QdbInteger integer = cluster.integer(alias);
        cluster.close();
        integer.put(666); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwReservedAlias() {
        QdbInteger integer = Helpers.getInteger(Helpers.RESERVED_ALIAS);
        integer.put(666); // <- throws
    }
}
