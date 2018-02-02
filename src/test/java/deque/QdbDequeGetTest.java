import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbDequeGetTest {
    @Test
    public void returnsItemsInOrder_whenIndexIsPositive() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.pushBack(content1);
        deque.pushBack(content2);
        Buffer result1 = deque.get(0);
        Buffer result2 = deque.get(1);

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
        Buffer result1 = deque.get(-1);
        Buffer result2 = deque.get(-2);

        Assert.assertEquals(content2, result1.toByteBuffer());
        Assert.assertEquals(content1, result2.toByteBuffer());
    }

    @Test(expected = AliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbDeque deque = Helpers.createEmptyDeque();

        deque.get(0); // <- throws
    }

    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        QdbDeque deque = cluster.deque(alias);
        cluster.close();
        deque.get(0); // <- throws
    }

    @Test(expected = IncompatibleTypeException.class)
    public void throwsIncompatibleTypeFound_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbDeque deque = Helpers.getDeque(alias);

        integer.put(666);
        deque.get(0); // <- throws
    }

    @Test(expected = OutOfBoundsException.class)
    public void throwsOutOfBounds_whenIndexIsGreaterThanSize() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content = Helpers.createSampleData();

        deque.pushBack(content);
        deque.get(1); // <- throws
    }

    @Test(expected = OutOfBoundsException.class)
    public void throwsOutOfBounds_whenIndexIsLowerThanNegatedSize() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content = Helpers.createSampleData();

        deque.pushBack(content);
        deque.get(-2); // <- throws
    }

    @Test(expected = ReservedAliasException.class)
    public void throwsReservedAlias() {
        QdbDeque deque = Helpers.getDeque(Helpers.RESERVED_ALIAS);
        deque.get(0); // <- throws
    }
}
