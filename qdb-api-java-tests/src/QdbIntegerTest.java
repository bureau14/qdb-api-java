import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbIntegerTest {
    private QdbCluster cluster;

    @Test
    public void add_returnsUpdatedValue() throws QdbException {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(19);
        long result = integer.add(23);

        Assert.assertEquals(42, result);
    }

    @Test
    public void get_returnsOriginalValue_afterCallingPut() throws QdbException {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(42);
        long result = integer.get();

        Assert.assertEquals(42, result);
    }

    @Test
    public void get_returnsUpdatedValue_afterCallingSet() throws QdbException {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(666);
        integer.set(42);
        long result = integer.get();

        Assert.assertEquals(42, result);
    }

    @Test
    public void get_returnsUpdatedValue_afterCallingGetAndSet() throws QdbException {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(666);
        integer.getAndSet(42);
        long result = integer.get();

        Assert.assertEquals(42, result);
    }

    @Test
    public void get_returnsUpdatedValue_afterCallingAdd() throws QdbException {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(19);
        integer.add(23);
        long result = integer.get();

        Assert.assertEquals(42, result);
    }

    @Test
    public void get_returnsZero_afterCallingPutWithZeroArgs() throws QdbException {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put();
        long result = integer.get();

        Assert.assertEquals(0, result);
    }

    @Test(expected = QdbException.class)
    public void get_throws_afterCallingPutAndWaitingExpiration() throws QdbException, InterruptedException {
        final long expiryTimeInSeconds = 1;
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(42, expiryTimeInSeconds);
        Thread.sleep((expiryTimeInSeconds + 1) * 1000);
        integer.get(); // <- throws
    }

    @Test(expected = QdbException.class)
    public void get_throws_afterCallingExpiresFromNowWithZero() throws QdbException, InterruptedException {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(1);
        integer.expiresFromNow(0);
        Thread.sleep(500L);
        integer.get(); // <- throws
    }

    @Test(expected = QdbException.class)
    public void get_throws_afterCallingRemove() throws QdbException {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(1);
        integer.remove();
        integer.get(); // <- throws
    }

    @Test
    public void getAlias_returnsProvidedAlias() throws QdbException {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);

        String result = integer.getAlias();

        Assert.assertEquals(alias, result);
    }

    @Test
    public void getAndSet_returnsOriginalValue() throws QdbException {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(42);
        long result = integer.getAndSet(666);

        Assert.assertEquals(42, result);
    }

    @Test
    public void getExpiryTime_returnSameValue_afterCallingExpiresAt() throws QdbException {
        QdbInteger integer = Helpers.createEmptyInteger();
        long expiryTimeInMillis = System.currentTimeMillis() + 1000 * 60 * 60;
        long expiryTimeInSeconds = expiryTimeInMillis / 1000;
        Date expiryDate = new Date(expiryTimeInMillis);

        integer.put(1);
        integer.expiresAt(expiryDate);
        long result = integer.getExpiryTime();

        Assert.assertEquals(expiryTimeInSeconds, result);
    }

    @Test
    public void getExpiryTime_returnsZero_afterCallingPutWithoutExpiration() throws QdbException {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(42);
        long result = integer.getExpiryTime();

        Assert.assertEquals(0, result);
    }
}
