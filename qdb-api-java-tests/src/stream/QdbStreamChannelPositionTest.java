import java.nio.ByteBuffer;
import java.nio.channels.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbStreamChannelPositionTest {
    @Test(expected = ClosedChannelException.class)
    public void getter_throwsClosedChannel_afterCallingClose() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.close();
        channel.position(); // <- throws
    }

    @Test
    public void getter_returnsZero_whenStreamIsNew() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        long result = channel.position();

        Assert.assertEquals(0, result);
    }

    @Test
    public void getter_returnsZero_whenStreamIsNotEmpty() throws Exception {
        QdbStream stream = Helpers.createStream();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.READ);
        long result = channel.position();

        Assert.assertEquals(0, result);
    }

    @Test
    public void getter_returnsSize_afterWrite() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.write(content);
        long result = channel.position();

        Assert.assertEquals(content.limit(), result);
    }

    @Test
    public void getter_returnsSize_afterRead() throws Exception {
        QdbStream stream = Helpers.createStream();
        ByteBuffer destination = ByteBuffer.allocateDirect(4);

        SeekableByteChannel channel = stream.open(QdbStream.Mode.READ);
        channel.read(destination);
        long result = channel.position();

        Assert.assertEquals(4, result);
    }

    @Test
    public void getter_returnsSameValue_afterCallingSetter() throws Exception {
        QdbStream stream = Helpers.createStream();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.READ);
        channel.position(4);
        long result = channel.position();

        Assert.assertEquals(4, result);
    }

    @Test(expected = ClosedChannelException.class)
    public void setter_throwsClosedChannel_afterCallingClose() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.close();
        channel.position(0); // <- throws
    }

    @Test(expected = IllegalArgumentException.class)
    public void setter_throwsIllegalArgument_whenValueIsNegative() throws Exception {
        QdbStream stream = Helpers.createStream();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.READ);
        channel.position(-1); // <- throws
    }
}
