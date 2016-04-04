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
    public void throwReservedAlias() {
        QdbInteger integer = Helpers.getInteger("qdb");
        integer.update(666); // <- throws
    }
}
