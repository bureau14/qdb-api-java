import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbExpiryTimeTest {
    @Test
    public void toSecondsSinceEpoch_after_makeSecondsSinceEpoch() {
        QdbExpiryTime expiry = QdbExpiryTime.makeSecondsSinceEpoch(42);
        long result = expiry.toSecondsSinceEpoch();

        Assert.assertEquals(42, result);
    }

    @Test
    public void toSecondsSinceEpoch_after_makeSecondsFromNow() {
        long timeBefore = System.currentTimeMillis() / 1000;
        QdbExpiryTime expiry = QdbExpiryTime.makeSecondsFromNow(42);
        long result = expiry.toSecondsSinceEpoch();
        long timeAfter = System.currentTimeMillis() / 1000;

        Assert.assertTrue(timeBefore + 42 <= result);
        Assert.assertTrue(timeAfter + 42 >= result);
    }

    @Test
    public void toSecondsSinceEpoch_after_makeMillisFromNow() {
        long timeBefore = System.currentTimeMillis() / 1000;
        QdbExpiryTime expiry = QdbExpiryTime.makeMillisFromNow(42000);
        long result = expiry.toSecondsSinceEpoch();
        long timeAfter = System.currentTimeMillis() / 1000;

        Assert.assertTrue(timeBefore + 42 <= result);
        Assert.assertTrue(timeAfter + 42 >= result);
    }

    @Test
    public void toSecondsSinceEpoch_after_makeMillisSinceEpoch() {
        QdbExpiryTime expiry = QdbExpiryTime.makeMillisSinceEpoch(42000);
        long result = expiry.toSecondsSinceEpoch();

        Assert.assertEquals(42, result);
    }

    @Test
    public void toSecondsSinceEpoch_after_fromDate() {
        Date date = new Date(1457023740000L);

        QdbExpiryTime expiry = QdbExpiryTime.fromDate(date);
        long result = expiry.toSecondsSinceEpoch();

        Assert.assertEquals(1457023740, result);
    }

    @Test
    public void toSecondsSinceEpoch_after_fromCalendar() {
        Calendar calendar = new GregorianCalendar();
        calendar.setTimeInMillis(1457025181000L);

        QdbExpiryTime expiry = QdbExpiryTime.fromCalendar(calendar);
        long result = expiry.toSecondsSinceEpoch();

        Assert.assertEquals(1457025181, result);
    }

    @Test
    public void toDate_after_makeSecondsSinceEpoch() {
        QdbExpiryTime expiry = QdbExpiryTime.makeSecondsSinceEpoch(1457026718);
        Date result = expiry.toDate();

        Assert.assertEquals(1457026718000L, result.getTime());
    }

    @Test
    public void toCalendar_after_makeSecondsSinceEpoch() {
        QdbExpiryTime expiry = QdbExpiryTime.makeSecondsSinceEpoch(1457026998);
        Calendar result = expiry.toCalendar();

        Assert.assertEquals(1457026998000L, result.getTimeInMillis());
    }
}
