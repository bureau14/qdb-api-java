import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;
import org.junit.rules.*;

public class QdbStreamWriteTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void throwsWhenWritingToClosedStream() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        exception.expect(QdbInvalidArgumentException.class);
        stream.write(content);
    }

    @Test
    public void writesToStream() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        stream.open(StandardOpenOption.APPEND);
        stream.write(content);
        Assert.assertEquals(content.limit(), stream.size());
    }

    @Test
    public void writesToStreamManyTimes() {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        stream.open(StandardOpenOption.APPEND);
        for (int i = 0; i < 5; ++i) {
            Assert.assertEquals(i * content.limit(), stream.size());
            stream.write(content);
            Assert.assertEquals((i + 1) * content.limit(), stream.size());
        }
    }

}
