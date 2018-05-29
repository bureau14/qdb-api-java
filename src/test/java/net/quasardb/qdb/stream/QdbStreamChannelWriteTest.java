import java.nio.ByteBuffer;
import java.nio.channels.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbStreamChannelWriteTest {
    @Test(expected = ClosedChannelException.class)
    public void throwsClosedChannel_afterCallingClose() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        ByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.close();
        channel.write(content); // <- throws
    }

    @Test(expected = NonWritableChannelException.class)
    public void throwsNonWritableChannel_whenOpenedInReadMode() throws Exception {
        QdbStream stream = Helpers.createStream();
        ByteBuffer content = Helpers.createSampleData();

        ByteChannel channel = stream.open(QdbStream.Mode.READ);
        channel.write(content); // <- throws
    }
}
