import java.nio.ByteBuffer;
import java.nio.channels.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbStreamChannelReadTest {
    @Test(expected = ClosedChannelException.class)
    public void throwsClosedChannel_afterCallingClose() throws Exception {
        QdbStream stream = Helpers.createStream();
        ByteBuffer destination = ByteBuffer.allocateDirect(100);

        ByteChannel channel = stream.open(QdbStream.Mode.READ);
        channel.close();
        channel.read(destination); // <- throws
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIllegalArgument_whenBufferIsNull() throws Exception {
        QdbStream stream = Helpers.createStream();

        ByteChannel channel = stream.open(QdbStream.Mode.READ);
        channel.read(null); // <- throws
    }

    @Test
    public void returnsRemainingSize_whenStreamIsBigger() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = ByteBuffer.allocateDirect(666);
        ByteBuffer destination = ByteBuffer.allocateDirect(66);

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.write(content);
        channel.position(0);
        destination.position(24);
        int result = channel.read(destination);

        Assert.assertEquals(42, result);
    }

    @Test
    public void returnsStreamSize_whenBufferIsBigger() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = ByteBuffer.allocateDirect(42);
        ByteBuffer destination = ByteBuffer.allocateDirect(666);

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.write(content);
        channel.position(0);
        int result = channel.read(destination);

        Assert.assertEquals(42, result);
    }

    @Test
    public void returnsMinusOne_whenEndOfStream() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = ByteBuffer.allocateDirect(42);
        ByteBuffer destination = ByteBuffer.allocateDirect(42);

        ByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.write(content);
        int result = channel.read(destination);

        Assert.assertEquals(-1, result);
    }

    @Test
    public void returnsZero_whenRemainingSizeIsZero() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer destination = ByteBuffer.allocateDirect(42);

        ByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        destination.position(42);
        int result = channel.read(destination);

        Assert.assertEquals(0, result);
    }

    @Test
    public void updatesDestinationBuffer() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();
        ByteBuffer destination = Helpers.createSampleData(content.limit());

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.write(content);
        channel.position(0);
        channel.read(destination);

        Assert.assertEquals(content, destination);
    }

    @Test
    public void moveDestinationPositionToLimit_whenStreamIsBigger() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = ByteBuffer.allocateDirect(666);
        ByteBuffer destination = ByteBuffer.allocateDirect(42);

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.write(content);
        channel.position(0);
        destination.position(5);
        channel.read(destination);

        Assert.assertEquals(42, destination.position());
    }

    @Test
    public void moveDestinationPosition_whenStreamIsSmaller() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = ByteBuffer.allocateDirect(42);
        ByteBuffer destination = ByteBuffer.allocateDirect(666);

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.write(content);
        channel.position(0);
        destination.position(5);
        channel.read(destination);

        Assert.assertEquals(42 + 5, destination.position());
    }
}
