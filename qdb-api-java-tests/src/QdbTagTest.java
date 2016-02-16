import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbTagTest {
    @Test
    public void testTag() throws QdbException {
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
        List<String> tags = blob.getTags();
        Assert.assertEquals(tags.size(), 1);
        Assert.assertEquals(tags.get(0), tagAlias);

        // reverse lookup must work
        List<String> entries = tag.getEntries();
        Assert.assertEquals(entries.size(), 1);
        Assert.assertEquals(entries.get(0), blobAlias);

        // cannot remove tag twice
        Assert.assertTrue(blob.removeTag(tagAlias));
        Assert.assertFalse(blob.removeTag(tagAlias));

        Assert.assertFalse(blob.hasTag(tagAlias));
    }
}
