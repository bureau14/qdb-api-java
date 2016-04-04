import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbDequeGetTest {
    @Test
    public void returnsItemsInOrder_whenIndexIsPositive() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.pushBack(content1);
        deque.pushBack(content2);
        QdbBuffer result1 = deque.get(0);
        QdbBuffer result2 = deque.get(1);

        Assert.assertEquals(content1, result1.toByteBuffer());
        Assert.assertEquals(content2, result2.toByteBuffer());
    }

    @Test
    public void returnsItemsInReverseOrder_whenIndexIsNegative() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.pushBack(content1);
        deque.pushBack(content2);
        QdbBuffer result1 = deque.get(-1);
        QdbBuffer result2 = deque.get(-2);

        Assert.assertEquals(content2, result1.toByteBuffer());
        Assert.assertEquals(content1, result2.toByteBuffer());
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbDeque deque = Helpers.createEmptyDeque();

        deque.get(0); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        QdbDeque deque = cluster.deque(alias);
        cluster.close();
        deque.get(0); // <- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleTypeFound_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbDeque deque = Helpers.getDeque(alias);

        integer.put(666);
        deque.get(0); // <- throws
    }

    @Test(expected = QdbOutOfBoundsException.class)
    public void throwsOutOfBounds_whenIndexIsGreaterThanSize() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content = Helpers.createSampleData();

        deque.pushBack(content);
        deque.get(1); // <- throws
    }

    @Test(expected = QdbOutOfBoundsException.class)
    public void throwsOutOfBounds_whenIndexIsLowerThanNegatedSize() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content = Helpers.createSampleData();

        deque.pushBack(content);
        deque.get(-2); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        QdbDeque deque = Helpers.getDeque("qdb");
        deque.get(0); // <- throws
    }
}
