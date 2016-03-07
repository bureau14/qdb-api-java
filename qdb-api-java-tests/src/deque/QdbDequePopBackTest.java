import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbDequePopBackTest {
    @Test
    public void returnItemsInReverseOrder_afterCallingPushBack() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.pushBack(content1);
        deque.pushBack(content2);
        ByteBuffer result1 = deque.popBack();
        ByteBuffer result2 = deque.popBack();

        Assert.assertEquals(content2, result1);
        Assert.assertEquals(content1, result2);
    }

    @Test
    public void returnsNull_whenDequeIsEmpty() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content = Helpers.createSampleData();

        deque.pushBack(content);
        deque.popBack();
        ByteBuffer result = deque.popBack();

        Assert.assertNull(result);
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbDeque deque = Helpers.createEmptyDeque();

        deque.popBack(); // <- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleTypeFound_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbDeque deque = Helpers.getDeque(alias);

        integer.put(666);
        deque.popBack(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        QdbDeque deque = Helpers.getDeque("qdb");
        deque.popBack(); // <- throws
    }
}
