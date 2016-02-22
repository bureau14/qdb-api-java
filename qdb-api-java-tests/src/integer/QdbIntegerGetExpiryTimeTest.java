import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbIntegerGetExpiryTimeTest {
    @Test
    public void returnSameValue_afterCallingExpiresAt() {
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
    public void returnsZero_afterCallingPutWithoutExpiration() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(42);
        long result = integer.getExpiryTime();

        Assert.assertEquals(0, result);
    }
}
