import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbTagEntriesTest {
    @Test
    public void returnsEmptyCollection_whenAliasIsRandom() {
        QdbTag tag = Helpers.createEmptyTag();

        Iterable<QdbEntry> result = tag.entries();

        List<QdbEntry> resultAsList = Helpers.toList(result);
        Assert.assertEquals(0, resultAsList.size());
    }

    @Test
    public void returnsEmptyCollection_afterCallingRemoveTag() {
        QdbBlob blob = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        blob.addTag(tag);
        blob.removeTag(tag);
        Iterable<QdbEntry> result = tag.entries();

        List<QdbEntry> resultAsList = Helpers.toList(result);
        Assert.assertEquals(0, resultAsList.size());
    }

    @Test
    public void returnsEmptyCollection_afterCallingRemoveEntry() {
        QdbBlob blob = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        tag.addEntry(blob);
        tag.removeEntry(blob);
        Iterable<QdbEntry> result = tag.entries();

        List<QdbEntry> resultAsList = Helpers.toList(result);
        Assert.assertEquals(0, resultAsList.size());
    }

    @Test
    public void returnsABlob_afterCallingAddTag() {
        QdbBlob blob = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        blob.addTag(tag);
        Iterable<QdbEntry> result = tag.entries();

        List<QdbEntry> resultAsList = Helpers.toList(result);
        Assert.assertEquals(1, resultAsList.size());
        Assert.assertEquals(blob.alias(), resultAsList.get(0).alias());
        Assert.assertTrue(resultAsList.get(0) instanceof QdbBlob);
    }

    @Test
    public void returnsABlob_afterCallingAddEntry() {
        QdbBlob blob = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        tag.addEntry(blob);
        Iterable<QdbEntry> result = tag.entries();

        List<QdbEntry> resultAsList = Helpers.toList(result);
        Assert.assertEquals(1, resultAsList.size());
        Assert.assertEquals(blob.alias(), resultAsList.get(0).alias());
        Assert.assertTrue(resultAsList.get(0) instanceof QdbBlob);
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenTagIsQdb() {
        QdbTag tag = Helpers.getTag(Helpers.RESERVED_ALIAS);

        tag.entries().iterator(); // <- throws
    }
}
