import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbIntegerGetExpiryTimeTest {
    @Test
    public void returnSameValue_afterCallingExpiresAt() {
        QdbInteger integer = Helpers.createEmptyInteger();
        QdbExpiryTime expiry = QdbExpiryTime.makeMinutesFromNow(5);

        integer.put(1);
        integer.setExpiryTime(expiry);
        QdbExpiryTime result = integer.getExpiryTime();

        Assert.assertEquals(expiry, result);
    }

    @Test
    public void returnsZero_afterCallingPutWithoutExpiration() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(42);
        QdbExpiryTime result = integer.getExpiryTime();

        Assert.assertEquals(QdbExpiryTime.NEVER_EXPIRES, result);
    }
}
