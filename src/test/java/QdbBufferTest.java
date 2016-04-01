import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBufferTest {
    @Test(expected = QdbBufferClosedException.class)
    public void toByteBuffer_throwsAfterCallingClose() {
        QdbBlob blob = Helpers.createBlob();
        QdbBuffer buffer = blob.get();

        buffer.close();
        buffer.toByteBuffer(); // <- throws
    }
}
