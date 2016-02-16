import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbHashSetTest {
    @Test
    public void getAlias_returnsSameAlias() throws QdbException {
        String alias = Helpers.createUniqueAlias();
        QdbHashSet hset = Helpers.getHashSet(alias);

        String result = hset.getAlias();

        Assert.assertEquals(alias, result);
    }

    @Test
    public void contains_returnTrue_afterCallingInsert() throws QdbException {
        QdbHashSet hset = Helpers.createEmptyHashSet();
        ByteBuffer content = Helpers.createSampleData();

        hset.insert(content);
        boolean result = hset.contains(content);

        Assert.assertTrue(result);
    }

    @Test
    public void contains_returnFalse_afterCallingErase() throws QdbException {
        QdbHashSet hset = Helpers.createEmptyHashSet();
        ByteBuffer content = Helpers.createSampleData();

        hset.insert(content);
        hset.erase(content);
        boolean result = hset.contains(content);

        Assert.assertFalse(result);
    }

    @Test
    public void insert_returnTrue() throws QdbException {
        QdbHashSet hset = Helpers.createEmptyHashSet();
        ByteBuffer content = Helpers.createSampleData();

        boolean result = hset.insert(content);

        Assert.assertTrue(result);
    }

    @Test
    public void insert_returnFalse_whenCalledTwice() throws QdbException {
        QdbHashSet hset = Helpers.createEmptyHashSet();
        ByteBuffer content = Helpers.createSampleData();

        hset.insert(content);
        boolean result = hset.insert(content);

        Assert.assertFalse(hset.insert(content));
    }

    @Test
    public void erase_returnsFalse_whenCalledTwice() throws QdbException {
        QdbHashSet hset = Helpers.createEmptyHashSet();
        ByteBuffer content = Helpers.createSampleData();

        hset.insert(content);
        hset.erase(content);
        boolean result = hset.erase(content);

        Assert.assertFalse(result);
    }

    @Test
    public void erase_returnsFalse_afterCallingInsert() throws QdbException {
        QdbHashSet hset = Helpers.createEmptyHashSet();
        ByteBuffer content = Helpers.createSampleData();

        hset.insert(content);
        boolean result = hset.erase(content);

        Assert.assertTrue(result);
    }
}
