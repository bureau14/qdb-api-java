import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbDequeFrontTest {
    @Test
    public void returnsFirstItem_afterCallingPushBack() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.pushBack(content1);
        deque.pushBack(content2);
        QdbBuffer result1 = deque.front();
        QdbBuffer result2 = deque.front();

        Assert.assertEquals(content1, result1.toByteBuffer());
        Assert.assertEquals(content1, result2.toByteBuffer());
    }

    @Test
    public void returnsNull_whenDequeIsEmpty() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content = Helpers.createSampleData();

        deque.pushBack(content);
        deque.popBack();
        QdbBuffer result = deque.front();

        Assert.assertNull(result);
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbDeque deque = Helpers.createEmptyDeque();

        deque.front(); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        QdbDeque deque = cluster.deque(alias);
        cluster.close();
        deque.front(); // <- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleTypeFound_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbDeque deque = Helpers.getDeque(alias);

        integer.put(666);
        deque.front(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        QdbDeque deque = Helpers.getDeque(Helpers.RESERVED_ALIAS);
        deque.front(); // <- throws
    }
}
