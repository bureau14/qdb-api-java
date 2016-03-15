import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.io.IOException;
import net.quasardb.qdb.*;

public class Helpers {
    private static QdbCluster cluster = createCluster();
    private static long n;

    private static QdbCluster createCluster() {
        try {
            return new QdbCluster(DaemonRunner.getURI());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void wait(double seconds) {
        try {
            Thread.sleep((long)(seconds * 1000));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static ByteBuffer createSampleData() {
        return createSampleData(0);
    }

    public static ByteBuffer createSampleData(int size) {
        String DATA = String.format("data.%d", n++);
        if (size == 0)
            size = DATA.getBytes().length;
        ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        buffer.put(DATA.getBytes());
        buffer.flip();
        return buffer;
    }

    public static QdbBatch createBatch() {
        return cluster.createBatch();
    }

    public static String createUniqueAlias() {
        return String.format("test.%d", n++);
    }

    public static QdbBlob createEmptyBlob() {
        return getBlob(createUniqueAlias());
    }

    public static QdbBlob createBlob() {
        QdbBlob blob = createEmptyBlob();
        blob.put(createSampleData());
        return blob;
    }

    public static QdbDeque createEmptyDeque() {
        return getDeque(createUniqueAlias());
    }

    public static QdbHashSet createEmptyHashSet() {
        return getHashSet(createUniqueAlias());
    }

    public static QdbInteger createEmptyInteger() {
        return getInteger(createUniqueAlias());
    }

    public static QdbStream createEmptyStream() {
        return getStream(createUniqueAlias());
    }

    public static QdbStream createStream() throws ClosedChannelException, IOException {
        QdbStream stream = createEmptyStream();
        try (SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND)) {
            channel.write(createSampleData());
        }
        return stream;
    }

    public static QdbBlob getBlob(String alias) {
        return cluster.blob(alias);
    }

    public static QdbCluster getCluster() {
        return cluster;
    }

    public static QdbDeque getDeque(String alias) {
        return cluster.deque(alias);
    }

    public static QdbHashSet getHashSet(String alias) {
        return cluster.hashSet(alias);
    }

    public static QdbInteger getInteger(String alias) {
        return cluster.integer(alias);
    }

    public static QdbNode getNode() {
        return cluster.node(DaemonRunner.getURI());
    }

    public static QdbStream getStream(String alias) {
        return cluster.stream(alias);
    }

    public static QdbTag getTag(String alias) {
        return cluster.tag(alias);
    }
}
