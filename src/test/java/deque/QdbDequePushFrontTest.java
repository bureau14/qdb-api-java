import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbDequePushFrontTest {
    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();
        ByteBuffer content = Helpers.createSampleData();

        QdbDeque deque = cluster.deque(alias);
        cluster.close();
        deque.pushFront(content); // <- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleTypeFound_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbDeque deque = Helpers.getDeque(alias);
        ByteBuffer content = Helpers.createSampleData();

        integer.put(666);
        deque.pushFront(content); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        ByteBuffer content = Helpers.createSampleData();

        QdbDeque deque = Helpers.getDeque("qdb");
        deque.pushFront(content); // <- throws
    }
}
