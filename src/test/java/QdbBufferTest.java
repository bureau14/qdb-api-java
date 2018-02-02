import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;
import java.nio.ByteBuffer;

public class QdbBufferTest {
    @Test(expected = BufferClosedException.class)
    public void toByteBuffer_throwsBufferClosedException_afterBufferClose() {
        QdbBlob blob = Helpers.createBlob();
        Buffer buffer = blob.get();

        buffer.close();
        buffer.toByteBuffer(); // <- throws
    }

    @Test(expected = ClusterClosedException.class)
    public void toByteBuffer_throwsClusterClosedException_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        QdbBlob blob = cluster.blob(Helpers.createUniqueAlias());
        blob.put(Helpers.createSampleData());

        Buffer buffer = blob.get();
        cluster.close();
        buffer.toByteBuffer(); // <- throws
    }
}
