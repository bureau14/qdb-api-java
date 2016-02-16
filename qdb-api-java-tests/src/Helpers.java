import java.nio.ByteBuffer;
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
        String DATA = String.format("data.%d", n++);
        ByteBuffer buffer = ByteBuffer.allocateDirect(DATA.getBytes().length);
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

    public static QdbDeque createEmptyDeque() {
        return getDeque(createUniqueAlias());
    }

    public static QdbHashSet createEmptyHashSet() {
        return getHashSet(createUniqueAlias());
    }

    public static QdbInteger createEmptyInteger() {
        return getInteger(createUniqueAlias());
    }

    public static QdbBlob getBlob(String alias) {
        return cluster.getBlob(alias);
    }

    public static QdbDeque getDeque(String alias) {
        return cluster.getDeque(alias);
    }

    public static QdbHashSet getHashSet(String alias) {
        return cluster.getSet(alias);
    }

    public static QdbInteger getInteger(String alias) {
        return cluster.getInteger(alias);
    }

    public static QdbTag getTag(String alias) {
        return cluster.getTag(alias);
    }
}