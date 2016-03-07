import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;
import org.junit.rules.*;

public class QdbStreamTruncateTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void throwsInvalidArgumentOnClosedStream() {
        QdbStream stream = Helpers.createEmptyStream();

        exception.expect(QdbInvalidArgumentException.class);
        stream.truncate(0);
    }

    @Test
    public void throwsInvalidArgumentOnOpenedAndClosedStream() {
        QdbStream stream = Helpers.createEmptyStream();

        stream.open(StandardOpenOption.APPEND);
        stream.close();

        exception.expect(QdbInvalidArgumentException.class);
        stream.truncate(0);
    }

    @Test
    public void truncatesEmptyStream() {
        QdbStream stream = Helpers.createEmptyStream();

        stream.open(StandardOpenOption.APPEND);
        stream.truncate(0);
        Assert.assertEquals(0, stream.size());
    }

    @Test
    public void throwsWhenTruncatingPastTheEnd() {
        QdbStream stream = Helpers.createEmptyStream();

        stream.open(StandardOpenOption.APPEND);
        exception.expect(QdbOutOfBoundsException.class);
        stream.truncate(1);
    }

    @Test
    public void truncatesSmallStreamAtZero() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        stream.open(StandardOpenOption.APPEND);
        stream.write(content);
        stream.truncate(0);
        Assert.assertEquals(0, stream.size());
    }

    @Test
    public void truncatesSmallStreamInTheMiddle() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        stream.open(StandardOpenOption.APPEND);
        stream.write(content);
        long sz = content.limit() / 2;
        stream.truncate(sz);
        Assert.assertEquals(sz, stream.size());
    }

    @Test
    public void truncatesSmallStreamAtTheEnd() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        stream.open(StandardOpenOption.APPEND);
        stream.write(content);
        stream.truncate(content.limit());
        Assert.assertEquals(content.limit(), stream.size());
    }

    @Test
    public void truncatesSmallStreamPastTheEnd() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        stream.open(StandardOpenOption.APPEND);
        stream.write(content);
        exception.expect(QdbOutOfBoundsException.class);
        stream.truncate(content.limit() + 1);
    }
}
