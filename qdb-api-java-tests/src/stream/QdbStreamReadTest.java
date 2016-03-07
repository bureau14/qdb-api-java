import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;
import org.junit.rules.*;

public class QdbStreamReadTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void throwsWhenReadingFromClosedStream() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer result = ByteBuffer.allocateDirect(100);

        Assert.assertNotNull(result);
        exception.expect(QdbInvalidArgumentException.class);
        stream.read(result);
    }

    @Test
    public void throwsNullPointerExceptionWhenResultBufferIsNull() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer result = null;

        stream.open(StandardOpenOption.APPEND);
        stream.write(content);
        stream.close();

        stream.open(StandardOpenOption.READ);
        Assert.assertNull(result);

        exception.expect(NullPointerException.class);
        stream.read(result);
    }

    @Test
    public void readsFromStream() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer result = ByteBuffer.allocateDirect(content.limit());
        Assert.assertEquals(content.capacity(), result.capacity());

        stream.open(StandardOpenOption.APPEND);
        stream.write(content);
        stream.close();

        stream.open(StandardOpenOption.READ);
        Assert.assertNotNull(result);

        Assert.assertEquals(content.limit(), stream.read(result));
        Assert.assertEquals(content.limit(), result.limit());
        Assert.assertEquals(content, result);
    }

    @Test
    public void readsFromStreamIntoBiggerBuffer() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer result = ByteBuffer.allocateDirect(100);
        Assert.assertTrue(content.capacity() <= result.capacity());

        stream.open(StandardOpenOption.APPEND);
        stream.write(content);
        stream.close();

        stream.open(StandardOpenOption.READ);
        Assert.assertNotNull(result);

        Assert.assertEquals(content.limit(), stream.read(result));
        result.limit(content.limit());
        Assert.assertEquals(content, result.slice());
    }
}
