import java.nio.ByteBuffer;
import java.nio.channels.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbStreamChannelSizeTest {
    @Test(expected = ClosedChannelException.class)
    public void throwsClosedChannel_afterCallingClose() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.close();
        channel.size(); // <- throws
    }

    @Test
    public void returnsZero_whenStreamIsEmpty() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        long result = channel.size();

        Assert.assertEquals(0, result);
    }

    @Test
    public void returnsContentSize_afterCallingWriteOnce() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.write(content);
        long result = channel.size();

        Assert.assertEquals(content.limit(), result);
    }

    @Test
    public void returnsContentSize_afterCallingWriteTwice() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.write(content);
        content.rewind();
        channel.write(content);
        long result = channel.size();

        Assert.assertEquals(content.limit() * 2, result);
    }

    @Test
    public void returnsUpdatedSize_afterCallingTruncate_withValueSmallerThanStream() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.write(content);
        channel.truncate(2);
        long result = channel.size();

        Assert.assertEquals(2, result);
    }

    @Test
    public void returnsOriginalSize_afterCallingTruncate_withValueLargerThanStream() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();
        ByteBuffer content = Helpers.createSampleData();

        SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND);
        channel.write(content);
        channel.truncate(666);
        long result = channel.size();

        Assert.assertEquals(content.limit(), result);
    }
}
