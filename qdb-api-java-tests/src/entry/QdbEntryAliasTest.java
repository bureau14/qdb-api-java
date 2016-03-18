import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbEntryAliasTest {
    @Test
    public void returnsSameAlias() {
        String alias = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.getBlob(alias);

        String result = entry.alias();

        Assert.assertEquals(alias, result);
    }
}
