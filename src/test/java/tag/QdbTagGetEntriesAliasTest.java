import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbTagGetEntriesAliasTest {
    @Test
    public void returnsEmptyCollection_whenAliasIsRandom() {
        QdbTag tag = Helpers.createEmptyTag();

        Iterable<String> result = tag.getEntriesAlias();

        List<String> resultAsList = Helpers.toList(result);
        Assert.assertEquals(0, resultAsList.size());
    }

    @Test
    public void returnOneAlias_afterCallingAddTag() {
        QdbBlob blob = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        blob.addTag(tag);
        Iterable<String> result = tag.getEntriesAlias();

        List<String> resultAsList = Helpers.toList(result);
        Assert.assertEquals(1, resultAsList.size());
        Assert.assertEquals(blob.alias(), resultAsList.get(0));
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenTagIsQdb() {
        QdbTag tag = Helpers.getTag(Helpers.RESERVED_ALIAS);

        tag.getEntriesAlias(); // <- throws
    }
}
