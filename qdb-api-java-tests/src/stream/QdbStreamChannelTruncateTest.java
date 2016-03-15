import java.nio.channels.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbStreamChannelTruncateTest {
    @Test(expected = ClosedChannelException.class)
    public void throwsClosedChannel_afterCallingClose() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.close();
        channel.truncate(0); // <- throws
    }

    @Test(expected = NonWritableChannelException.class)
    public void throwsNonWritableChannelException_whenOpenedInReadMode() throws Exception {
        QdbStream stream = Helpers.createStream();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.READ);
        channel.truncate(0); // <- throws
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIllegalArgumentException_whenValueIsNegative() throws Exception {
        QdbStream stream = Helpers.createStream();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.truncate(-1); // <- throws
    }

    @Test
    public void returnsSelf() throws Exception {
        QdbStream stream = Helpers.createStream();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        SeekableByteChannel result = channel.truncate(0);

        Assert.assertSame(channel, result);
    }
}
