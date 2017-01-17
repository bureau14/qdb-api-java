import net.quasardb.qdb.*;
import org.junit.*;

public class QdbIdTest {
    @Test
    public void equalsTrue() {
        QdbId id1 = new QdbId(3, 7, 20, 50);
        QdbId id2 = new QdbId(3, 7, 20, 50);

        Assert.assertEquals(id1, id1);
        Assert.assertEquals(id1, id2);
    }

    @Test
    public void equalsFalse() {
        QdbId id1 = new QdbId(3, 7, 20, 50);
        QdbId id2 = new QdbId(4, 7, 20, 50);

        Assert.assertNotEquals(id1, id2);
    }

    @Test
    public void toStringReturnsFourIntegers() {
        QdbId id = new QdbId(3, 7, 20, 50);

        Assert.assertEquals("[3-7-14-32]", id.toString());
    }
}
