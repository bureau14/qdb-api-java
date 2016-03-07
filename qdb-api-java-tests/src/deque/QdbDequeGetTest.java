import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbDequeGetTest {
    @Test
    public void returnsItems_afterCallingPushBack() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.pushBack(content1);
        deque.pushBack(content2);
        ByteBuffer result1 = deque.get(0);
        ByteBuffer result2 = deque.get(1);

        Assert.assertEquals(content1, result1);
        Assert.assertEquals(content2, result2);
    }

    @Test
    public void returnsItems_afterCallingPushFront() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.pushFront(content1);
        deque.pushFront(content2);
        ByteBuffer result1 = deque.get(0);
        ByteBuffer result2 = deque.get(1);

        Assert.assertEquals(content2, result1);
        Assert.assertEquals(content1, result2);
    }

    @Test(expected = QdbOutOfBoundsException.class)
    public void throwsOutOfBounds_whenIndexIsNegative() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content = Helpers.createSampleData();

        deque.pushBack(content);
        deque.get(-1); // <- throws
    }

    @Test(expected = QdbOutOfBoundsException.class)
    public void throwsOutOfBounds_whenIndexIsGreaterThanSize() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content = Helpers.createSampleData();

        deque.pushBack(content);
        deque.get(1); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbDeque deque = Helpers.createEmptyDeque();

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

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        QdbDeque deque = Helpers.getDeque("qdb");
        deque.get(0); // <- throws
    }
}
