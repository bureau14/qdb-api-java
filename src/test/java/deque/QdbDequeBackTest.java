import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbDequeBackTest {
    @Test
    public void returnsLastItem_afterCallingPushBack() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.pushBack(content1);
        deque.pushBack(content2);
        Buffer result1 = deque.back();
        Buffer result2 = deque.back();

        Assert.assertEquals(content2, result1.toByteBuffer());
        Assert.assertEquals(content2, result2.toByteBuffer());
    }

    @Test
    public void returnsNull_whenDequeIsEmpty() {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content = Helpers.createSampleData();

        deque.pushBack(content);
        deque.popBack();
        Buffer result = deque.back();

        Assert.assertNull(result);
    }

    @Test(expected = AliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbDeque deque = Helpers.createEmptyDeque();

        deque.back(); // <- throws
    }

    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        QdbDeque deque = cluster.deque(alias);
        cluster.close();
        deque.back(); // <- throws
    }

    @Test(expected = IncompatibleTypeException.class)
    public void throwsIncompatibleTypeFound_afterCallingIntegerPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbDeque deque = Helpers.getDeque(alias);

        integer.put(666);
        deque.back(); // <- throws
    }

    @Test(expected = ReservedAliasException.class)
    public void throwsReservedAlias() {
        QdbDeque deque = Helpers.getDeque(Helpers.RESERVED_ALIAS);
        deque.back(); // <- throws
    }
}
