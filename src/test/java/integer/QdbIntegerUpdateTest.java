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
        integer.update(666); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        QdbInteger integer = cluster.integer(alias);
        cluster.close();
        integer.update(666); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        QdbInteger integer = Helpers.getInteger(Helpers.RESERVED_ALIAS);
        integer.update(666); // <- throws
    }

    @Test
    public void returnsTrue_whenCalledOnce() {
        QdbInteger integer = Helpers.createEmptyInteger();

        boolean created = integer.update(42);

        Assert.assertTrue(created);
    }

    @Test
    public void returnsFalse_whenCalledOnce() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.update(42);
        boolean created = integer.update(42);

        Assert.assertFalse(created);
    }
}
