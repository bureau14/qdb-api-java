import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbIntegerAddTest {
    @Test(expected = AliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.add(666); // <- throws
    }

    @Test(expected = ClusterClosedException.class)
    public void throwsClusterClosed_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        String alias = Helpers.createUniqueAlias();

        QdbInteger integer = cluster.integer(alias);
        cluster.close();
        integer.add(666); // <- throws
    }

    @Test(expected = IncompatibleTypeException.class)
    public void throwsIncompatibleType() {
        String alias = Helpers.createUniqueAlias();
        QdbBlob blob = Helpers.getBlob(alias);
        QdbInteger integer = Helpers.getInteger(alias);
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        integer.add(666); // <- throws
    }

    @Test(expected = OverflowException.class)
    public void throwsOverflow_whenAddingOneToMaxValue() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(Long.MAX_VALUE);
        integer.add(1); // <- throws
    }

    @Test(expected = UnderflowException.class)
    public void throwsUnderflow_whenSubtractingOneToMinValue() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(Long.MIN_VALUE);
        integer.add(-1); // <- throws
    }

    @Test(expected = ReservedAliasException.class)
    public void throwsReservedAlias() {
        QdbInteger integer = Helpers.getInteger(Helpers.RESERVED_ALIAS);
        integer.add(666); // <- throws
    }

    @Test
    public void returnsUpdatedValue() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(19);
        long result = integer.add(23);

        Assert.assertEquals(42, result);
    }
}
