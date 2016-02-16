import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbDequeTest {
    @Test
    public void getAlias_returnsSameAlias() throws QdbException {
        String alias = Helpers.createUniqueAlias();
        QdbDeque deque = Helpers.getDeque(alias);

        String result = deque.getAlias();

        Assert.assertEquals(alias, result);
    }

    @Test
    public void size_returnsOne_afterCallingAddFirst() throws QdbException {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content = Helpers.createSampleData();

        deque.addFirst(content);
        long result = deque.size();

        Assert.assertEquals(1, result);
    }

    @Test
    public void size_returnsOne_afterCallingAddLast() throws QdbException {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content = Helpers.createSampleData();

        deque.addLast(content);
        long result = deque.size();

        Assert.assertEquals(1, result);
    }

    @Test
    public void pollFirst_returnContentInOrder_afterCallingAddLast() throws QdbException {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.addLast(content1);
        deque.addLast(content2);
        ByteBuffer result1 = deque.pollFirst();
        ByteBuffer result2 = deque.pollFirst();

        Assert.assertEquals(content1, result1);
        Assert.assertEquals(content2, result2);
    }

    @Test
    public void pollLast_returnContentInReverseOrder_afterCallingAddLast() throws QdbException {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.addLast(content1);
        deque.addLast(content2);
        ByteBuffer result1 = deque.pollLast();
        ByteBuffer result2 = deque.pollLast();

        Assert.assertEquals(content2, result1);
        Assert.assertEquals(content1, result2);
    }

    @Test
    public void peekFirst_returnFirstContent_afterCallingAddLast() throws QdbException {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.addLast(content1);
        deque.addLast(content2);
        ByteBuffer result1 = deque.peekFirst();
        ByteBuffer result2 = deque.peekFirst();

        Assert.assertEquals(content1, result1);
        Assert.assertEquals(content1, result2);
    }

    @Test
    public void peekLast_returnLastContent_afterCallingAddLast() throws QdbException {
        QdbDeque deque = Helpers.createEmptyDeque();
        ByteBuffer content1 = Helpers.createSampleData();
        ByteBuffer content2 = Helpers.createSampleData();

        deque.addLast(content1);
        deque.addLast(content2);
        ByteBuffer result1 = deque.peekLast();
        ByteBuffer result2 = deque.peekLast();

        Assert.assertEquals(content2, result1);
        Assert.assertEquals(content2, result2);
    }
}
