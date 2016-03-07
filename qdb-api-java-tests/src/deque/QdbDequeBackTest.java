import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbDequeBackTest {
    @Test
    public void returnsLastItem_afterCallingPushBack() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.pushBack(content1);
        deque.pushBack(content2);
        ByteBuffer result1 = deque.back();
        ByteBuffer result2 = deque.back();

        Assert.assertEquals(content2, result1);
        Assert.assertEquals(content2, result2);
    }

    @Test
    public void returnsNull_whenDequeIsEmpty() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content = Helpers.createSampleData();

        deque.pushBack(content);
        deque.popBack();
        ByteBuffer result = deque.back();

        Assert.assertNull(result);
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbDeque deque = Helpers.createEmptyDeque();

        deque.back(); // <- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleTypeFound_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbDeque deque = Helpers.getDeque(alias);

        integer.put(666);
        deque.back(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        QdbDeque deque = Helpers.getDeque("qdb");
        deque.back(); // <- throws
    }
}
