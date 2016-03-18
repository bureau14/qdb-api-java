import java.nio.ByteBuffer;
import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbTagTest {
    @Test
    public void testTag() {
        String blobAlias = Helpers.createUniqueAlias();
        String tagAlias = Helpers.createUniqueAlias();
        QdbTag tag = Helpers.getTag(tagAlias);
        QdbBlob blob = Helpers.getBlob(blobAlias);
        ByteBuffer content = Helpers.createSampleData();

        blob.put(content);

        Assert.assertFalse(blob.hasTag(tagAlias));

        // cannot add the tag twice
        Assert.assertTrue(blob.addTag(tagAlias));
        Assert.assertFalse(blob.addTag(tagAlias));

        // tag must be present
        Assert.assertTrue(blob.hasTag(tagAlias));

        // tag must be listed
        List<String> tags = Helpers.toList(blob.getTagsAlias());
        Assert.assertEquals(tags.size(), 1);
        Assert.assertEquals(tags.get(0), tagAlias);

        // reverse lookup must work
        List<String> entries = Helpers.toList(tag.getEntriesAlias());
        Assert.assertEquals(entries.size(), 1);
        Assert.assertEquals(entries.get(0), blobAlias);

        // cannot remove tag twice
        Assert.assertTrue(blob.removeTag(tagAlias));
        Assert.assertFalse(blob.removeTag(tagAlias));

        Assert.assertFalse(blob.hasTag(tagAlias));
    }

    @Test
    public void getEntriesAlias_returnsEmptyCollection_whenAliasIsRandom() {
        QdbTag tag = Helpers.createEmptyTag();

        Iterable<String> result = tag.getEntriesAlias();

        List<String> resultAsList = Helpers.toList(result);
        Assert.assertEquals(0, resultAsList.size());
    }

    public void getEntriesAlias_returnOneAlias_afterCallingAddTag() {
        QdbBlob blob = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        blob.addTag(tag);
        Iterable<String> result = tag.getEntriesAlias();

        List<String> resultAsList = Helpers.toList(result);
        Assert.assertEquals(0, resultAsList.size());
        Assert.assertEquals(blob.alias(), resultAsList.get(0));
    }

    @Test
    public void getEntries_returnsEmptyCollection_whenAliasIsRandom() {
        QdbTag tag = Helpers.createEmptyTag();

        Iterable<QdbEntry> result = tag.getEntries();

        List<QdbEntry> resultAsList = Helpers.toList(result);
        Assert.assertEquals(0, resultAsList.size());
    }

    public void getEntries_returnOneAlias_afterCallingAddTag() {
        QdbBlob blob = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        blob.addTag(tag);
        Iterable<QdbEntry> result = tag.getEntries();

        List<QdbEntry> resultAsList = Helpers.toList(result);
        Assert.assertEquals(0, resultAsList.size());
        Assert.assertEquals(blob.alias(), resultAsList.get(0).alias());
        Assert.assertTrue(resultAsList.get(0) instanceof QdbBlob);
    }
}
