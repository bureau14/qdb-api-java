import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;
import org.junit.rules.*;

public class QdbStreamPositionTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void throwsInvalidArgumentOnClosedStreamGet() {
        QdbStream stream = Helpers.createEmptyStream();

        exception.expect(QdbInvalidArgumentException.class);
        stream.position();
    }

    @Test
    public void throwsInvalidArgumentOnClosedStreamSet() {
        QdbStream stream = Helpers.createEmptyStream();

        exception.expect(QdbInvalidArgumentException.class);
        stream.position(0);
    }

    @Test
    public void throwsInvalidArgumentOnOpenedAndClosedStreamGet() {
        QdbStream stream = Helpers.createEmptyStream();

        stream.open(StandardOpenOption.APPEND);
        stream.close();

        exception.expect(QdbInvalidArgumentException.class);
        stream.position();
    }

    @Test
    public void throwsInvalidArgumentOnOpenedAndClosedStreamSet() {
        QdbStream stream = Helpers.createEmptyStream();

        stream.open(StandardOpenOption.APPEND);
        stream.close();

        exception.expect(QdbInvalidArgumentException.class);
        stream.position(0);
    }

    @Test
    public void emptyStreamIsAtPositionZero_APPEND() {
        QdbStream stream = Helpers.createEmptyStream();

        stream.open(StandardOpenOption.APPEND);
        Assert.assertEquals(0, stream.position());
    }

    @Test
    public void smallOpenStreamGet() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        stream.open(StandardOpenOption.APPEND);
        stream.write(content);
        Assert.assertEquals(content.limit(), stream.position());
    }

    @Test
    public void smallOpenCloseOpenStreamGet() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        stream.open(StandardOpenOption.APPEND);
        stream.write(content);
        stream.close();

        stream.open(StandardOpenOption.READ);
        Assert.assertEquals(0, stream.position());
    }

    @Test
    public void smallOpenCloseOpenReadStreamGet() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        stream.open(StandardOpenOption.APPEND);
        stream.write(content);
        stream.close();

        ByteBuffer result = ByteBuffer.allocateDirect(100);
        stream.open(StandardOpenOption.READ);
        stream.read(result);

        Assert.assertEquals(content.limit(), stream.position());
    }

    @Test
    public void smallOpenStreamGetSet() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        stream.open(StandardOpenOption.APPEND);
        stream.write(content);
        Assert.assertEquals(0, stream.position(0).position());
        Assert.assertEquals(content.limit(), stream.position(content.limit()).position());
    }
}
