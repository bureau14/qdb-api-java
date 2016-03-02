import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbIntegerGetTest {
    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingPut_thenWaitingExpiration() {
        final long expiryTimeInSeconds = 1;
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(42, expiryTimeInSeconds);
        Helpers.wait(expiryTimeInSeconds + 0.5);
        integer.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingExpiresFromNowWithZero() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(1);
        integer.expiresFromNow(0);
        Helpers.wait(0.5);
        integer.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingRemove() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(1);
        integer.remove();
        integer.get(); // <- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingBlobPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbBlob blob = Helpers.getBlob(alias);
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        integer.get(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenAliasIsQdb() {
        QdbInteger integer = Helpers.getInteger("qdb");
        integer.get(); // <- throws
    }

    @Test
    public void returnsOriginalValue_afterCallingPut() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(42);
        long result = integer.get();

        Assert.assertEquals(42, result);
    }

    @Test
    public void returnsUpdatedValue_afterCallingSet() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(666);
        integer.set(42);
        long result = integer.get();

        Assert.assertEquals(42, result);
    }

    @Test
    public void returnsUpdatedValue_afterCallingAdd() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(19);
        integer.add(23);
        long result = integer.get();

        Assert.assertEquals(42, result);
    }
}
