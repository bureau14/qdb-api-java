import net.quasardb.qdb.*;
import org.junit.*;
import java.nio.ByteBuffer;

public class QdbBufferTest {
    @Test(expected = QdbBufferClosedException.class)
    public void toByteBuffer_throwsQdbBufferClosedException_afterQdbBufferClose() {
        QdbBlob blob = Helpers.createBlob();
        QdbBuffer buffer = blob.get();

        buffer.close();
        buffer.toByteBuffer(); // <- throws
    }

    @Test(expected = QdbClusterClosedException.class)
    public void toByteBuffer_throwsQdbClusterClosedException_afterCallingQdbClusterClose() {
        QdbCluster cluster = Helpers.createCluster();
        QdbBlob blob = cluster.blob(Helpers.createUniqueAlias());
        blob.put(Helpers.createSampleData());

        QdbBuffer buffer = blob.get();
        cluster.close();
        buffer.toByteBuffer(); // <- throws
    }
}
