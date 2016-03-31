import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbTagGetEntriesTest {
    @Test
    public void returnsEmptyCollection_whenAliasIsRandom() {
        QdbTag tag = Helpers.createEmptyTag();

        Iterable<QdbEntry> result = tag.getEntries();

        List<QdbEntry> resultAsList = Helpers.toList(result);
        Assert.assertEquals(0, resultAsList.size());
    }

    @Test
    public void returnOneAlias_afterCallingAddTag() {
        QdbBlob blob = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        blob.addTag(tag);
        Iterable<QdbEntry> result = tag.getEntries();

        List<QdbEntry> resultAsList = Helpers.toList(result);
        Assert.assertEquals(1, resultAsList.size());
        Assert.assertEquals(blob.alias(), resultAsList.get(0).alias());
        Assert.assertTrue(resultAsList.get(0) instanceof QdbBlob);
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenTagIsQdb() {
        QdbTag tag = Helpers.getTag(Helpers.RESERVED_ALIAS);

        tag.getEntries(); // <- throws
    }
}
