import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbIntegerAddTest {
    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.add(666); // <- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsIncompatibleType() {
        String alias = Helpers.createUniqueAlias();
        QdbBlob blob = Helpers.getBlob(alias);
        QdbInteger integer = Helpers.getInteger(alias);
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);
        integer.add(666); // <- throws
    }

    @Test(expected = QdbOverflowException.class)
    public void throwsOverflow_whenAddingOneToMaxValue() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(Long.MAX_VALUE);
        integer.add(1); // <- throws
    }

    @Test(expected = QdbUnderflowException.class)
    public void throwsUnderflow_whenSubtractingOneToMinValue() {
        QdbInteger integer = Helpers.createEmptyInteger();

        integer.put(Long.MIN_VALUE);
        integer.add(-1); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias() {
        QdbInteger integer = Helpers.getInteger("qdb");
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
