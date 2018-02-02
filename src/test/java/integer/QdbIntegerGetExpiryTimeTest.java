import net.quasardb.qdb.exception.*;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbIntegerGetExpiryTimeTest {
    @Test
    public void returnsSameValue_afterCallingPutWithExpiration() {
        QdbInteger integer = Helpers.createEmptyInteger();
        QdbExpiryTime expiry = QdbExpiryTime.makeMinutesFromNow(5);

        integer.put(42, expiry);
        QdbExpiryTime result = integer.expiryTime();

        Assert.assertEquals(expiry, result);
    }

    @Test
    public void returnOriginalValue_afterCallingUpdateWithoutExpiration() {
        QdbInteger integer = Helpers.createEmptyInteger();
        QdbExpiryTime expiry = QdbExpiryTime.makeMinutesFromNow(5);

        integer.put(666, expiry);
        integer.update(42);
        QdbExpiryTime result = integer.expiryTime();

        Assert.assertEquals(expiry, result);
    }

    @Test
    public void returnUpdatedValue_afterCallingUpdateWithExpiration() {
        QdbInteger integer = Helpers.createEmptyInteger();
        QdbExpiryTime expiry = QdbExpiryTime.makeMinutesFromNow(5);

        integer.put(666);
        integer.update(42, expiry);
        QdbExpiryTime result = integer.expiryTime();

        Assert.assertEquals(expiry, result);
    }

    @Test
    public void returnsZero_afterCallingPutWithoutExpiration() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(42);
        QdbExpiryTime result = integer.expiryTime();

        Assert.assertEquals(QdbExpiryTime.NEVER_EXPIRES, result);
    }
}
