import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;
import org.junit.rules.*;

public class QdbStreamSizeTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void throwsInvalidArgumentOnClosedStream() {
        QdbStream stream = Helpers.createEmptyStream();

        exception.expect(QdbInvalidArgumentException.class);
        stream.size();
    }

    @Test
    public void throwsInvalidArgumentOnOpenedAndClosedStream() {
        QdbStream stream = Helpers.createEmptyStream();

        stream.open(StandardOpenOption.APPEND);
        stream.close();

        exception.expect(QdbInvalidArgumentException.class);
        stream.size();
    }

    @Test
    public void emptyStreamHasZeroSize_APPEND() {
        QdbStream stream = Helpers.createEmptyStream();

        stream.open(StandardOpenOption.APPEND);
        Assert.assertEquals(0, stream.size());
    }

    @Test
    public void smallOpenStream() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        stream.open(StandardOpenOption.APPEND);
        stream.write(content);
        Assert.assertEquals(content.limit(), stream.size());
    }
}
