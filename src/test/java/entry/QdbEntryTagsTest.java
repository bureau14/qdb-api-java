import java.nio.ByteBuffer;
import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbEntryTagsTest {
    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenAliasIsQdb() {
        QdbEntry entry = Helpers.getBlob("qdb");

        entry.tags().iterator(); // <- throws
    }

    @Test
    public void returnsEmptyCollection_beforeCallingAddTag() {
        QdbEntry entry = Helpers.createBlob();

        Iterable<QdbTag> result = entry.tags();

        List<QdbTag> resultAsList = Helpers.toList(result);
        Assert.assertEquals(0, resultAsList.size());
    }

    @Test
    public void returnsOneTag_afterCallingAddTag() {
        QdbEntry entry = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        entry.addTag(tag);
        Iterable<QdbTag> result = entry.tags();

        List<QdbTag> resultAsList = Helpers.toList(result);
        Assert.assertEquals(1, resultAsList.size());
        Assert.assertEquals(tag.alias(), resultAsList.get(0).alias());
    }
}
