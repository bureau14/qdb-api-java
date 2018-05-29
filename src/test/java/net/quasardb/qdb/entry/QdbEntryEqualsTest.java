import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbEntryEqualsTest {
    @Test
    public void returnsFalse_withNull() {
        QdbEntry entry = Helpers.createEmptyBlob();

        boolean result = entry.equals(null);

        Assert.assertFalse(result);
    }

    @Test
    public void returnsFalse_whenTypesAreDifferent() {
        String alias = Helpers.createUniqueAlias();
        QdbEntry entry1 = Helpers.getBlob(alias);
        QdbEntry entry2 = Helpers.getInteger(alias);

        boolean result1 = entry1.equals(entry2);
        boolean result2 = entry2.equals(entry1);

        Assert.assertFalse(result1);
        Assert.assertFalse(result2);
    }

    @Test
    public void returnsFalse_whenAliasesAreDifferent() {
        QdbEntry entry1 = Helpers.createEmptyBlob();
        QdbEntry entry2 = Helpers.createEmptyBlob();

        boolean result1 = entry1.equals(entry2);
        boolean result2 = entry2.equals(entry1);

        Assert.assertFalse(result1);
        Assert.assertFalse(result2);
    }

    @Test
    public void returnsTrue_whenTypesAndAliasesMatch() {
        String alias = Helpers.createUniqueAlias();
        QdbEntry entry1 = Helpers.getBlob(alias);
        QdbEntry entry2 = Helpers.getBlob(alias);

        boolean result1 = entry1.equals(entry2);
        boolean result2 = entry2.equals(entry1);

        Assert.assertTrue(result1);
        Assert.assertTrue(result2);
    }

    @Test
    public void returnsTrue_withThis() {
        QdbEntry entry = Helpers.createEmptyBlob();

        boolean result = entry.equals(entry);

        Assert.assertTrue(result);
    }
}
