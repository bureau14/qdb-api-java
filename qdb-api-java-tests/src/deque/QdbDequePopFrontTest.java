import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbDequePopFrontTest {
    @Test
    public void returnsItemsInOrder_afterCallingPushBack() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.pushBack(content1);
        deque.pushBack(content2);
        ByteBuffer result1 = deque.popFront();
        ByteBuffer result2 = deque.popFront();

        Assert.assertEquals(content1, result1);
        Assert.assertEquals(content2, result2);
    }

    @Test
    public void returnsNull_whenDequeIsEmpty() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content = Helpers.createSampleData();

        deque.pushBack(content);
        deque.popFront();
        ByteBuffer result = deque.popFront();

        Assert.assertNull(result);
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbDeque deque = Helpers.createEmptyDeque();

        deque.popFront(); // <- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleTypeFound_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbDeque deque = Helpers.getDeque(alias);

        integer.put(666);
        deque.popFront(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        QdbDeque deque = Helpers.getDeque("qdb");
        deque.popFront(); // <- throws
    }
}