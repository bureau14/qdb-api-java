import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbIntegerGetTest {
    @Test(expected = AliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingPut_thenWaitingExpiration() {
        QdbInteger integer = Helpers.createEmptyInteger();
        QdbExpiryTime expiry = QdbExpiryTime.makeSecondsFromNow(1);

        integer.put(42, expiry);
        Helpers.wait(1.5);
        integer.get(); // <- throws
    }

    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        QdbInteger integer = cluster.integer(alias);
        cluster.close();
        integer.get(); // <- throws
    }

    @Test(expected = AliasNotFoundException.class)
    public void throwsAliasNotFound_afterCallingRemove() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(1);
        integer.remove();
        integer.get(); // <- throws
    }

    @Test(expected = IncompatibleTypeException.class)
    public void throwsIncompatibleType_afterCallingBlobPut() {
        String alias = Helpers.createUniqueAlias();
        QdbInteger integer = Helpers.getInteger(alias);
        QdbBlob blob = Helpers.getBlob(alias);
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        integer.get(); // <- throws
    }

    @Test(expected = ReservedAliasException.class)
    public void throwsReservedAlias_whenAliasIsQdb() {
        QdbInteger integer = Helpers.getInteger(Helpers.RESERVED_ALIAS);
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
        integer.update(42);
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
