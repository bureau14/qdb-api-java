import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import java.util.Date;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbExpirableEntryTest {
    @Test(expected = ReservedAliasException.class)
    @Ignore(value = "It's not clear what the right behavior should be.")
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

    @Test(expected = InvalidArgumentException.class)
    public void setExpiryTime_throwsInvalidArgument_whenDateIsInThePast() {
        QdbExpirableEntry entry = Helpers.createBlob();

        QdbExpiryTime fewMinutesAgo = QdbExpiryTime.makeMinutesFromNow(-7);
        entry.expiryTime(fewMinutesAgo);
    }
}
