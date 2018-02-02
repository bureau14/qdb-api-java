import net.quasardb.qdb.exception.*;
import java.nio.ByteBuffer;
import java.util.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbEntryTagsTest {
    // @Test(expected = ClusterClosedException.class)
    // public void throwsClusterClosed_afterCallingQdbClusterClose() {
    //     QdbCluster cluster = Helpers.createCluster();
    //     String alias = Helpers.createUniqueAlias();

    //     QdbEntry entry = cluster.blob(alias);
    //     cluster.close();
    //     entry.tags().iterator(); // <- throws
    // }

    @Test(expected = ReservedAliasException.class)
    public void throwsReservedAlias_whenAliasIsQdb() {
        QdbEntry entry = Helpers.getBlob(Helpers.RESERVED_ALIAS);

        entry.tags().iterator(); // <- throws
    }

    @Test
    public void returnsEmptyCollection_beforeCallingAttachTag() {
        QdbEntry entry = Helpers.createBlob();

        Iterable<QdbTag> result = entry.tags();

        List<QdbTag> resultAsList = Helpers.toList(result);
        Assert.assertEquals(0, resultAsList.size());
    }

    @Test
    public void returnsOneTag_afterCallingAttachTag() {
        QdbEntry entry = Helpers.createBlob();
        QdbTag tag = Helpers.createEmptyTag();

        entry.attachTag(tag);
        Iterable<QdbTag> result = entry.tags();

        List<QdbTag> resultAsList = Helpers.toList(result);
        Assert.assertEquals(1, resultAsList.size());
        Assert.assertEquals(tag.alias(), resultAsList.get(0).alias());
    }
}
