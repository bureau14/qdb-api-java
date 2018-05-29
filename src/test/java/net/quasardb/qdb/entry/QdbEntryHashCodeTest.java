import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbEntryHashCodeTest {
    @Test
    public void returnSameResult_whenEntryEquals() {
        String alias = Helpers.createUniqueAlias();
        QdbEntry entry1 = Helpers.getBlob(alias);
        QdbEntry entry2 = Helpers.getBlob(alias);

        int result1 = entry1.hashCode();
        int result2 = entry2.hashCode();

        Assert.assertTrue(result1 == result2);
    }

    @Test
    public void returnDifferentResult_whenAliasDiffers() {
        QdbEntry entry1 = Helpers.createEmptyBlob();
        QdbEntry entry2 = Helpers.createEmptyBlob();

        int result1 = entry1.hashCode();
        int result2 = entry2.hashCode();

        Assert.assertTrue(result1 != result2);
    }
}
