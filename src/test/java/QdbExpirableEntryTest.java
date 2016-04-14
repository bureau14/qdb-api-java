import java.nio.ByteBuffer;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbExpirableEntryTest {
    @Test(expected = QdbReservedAliasException.class)
    public void getExpiryTime_throwsReservedAlias() {
        QdbExpirableEntry entry = Helpers.getBlob(Helpers.RESERVED_ALIAS);
        entry.expiryTime(); // <- throws
    }

    @Test
    public void getExpiryTime_returnSameValue_afterCallingSetExpiryTime() {
        QdbExpirableEntry entry = Helpers.createBlob();
        QdbExpiryTime expiry = QdbExpiryTime.makeMinutesFromNow(1);

        entry.expiryTime(expiry);
        QdbExpiryTime result = entry.expiryTime();

        Assert.assertEquals(expiry, result);
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void setExpiryTime_throwsInvalidArgument_whenDateIsInThePast() {
        QdbExpirableEntry entry = Helpers.createBlob();

        QdbExpiryTime fiveMinutesAgo = QdbExpiryTime.makeMinutesFromNow(-5);
        entry.expiryTime(fiveMinutesAgo);
    }
}
