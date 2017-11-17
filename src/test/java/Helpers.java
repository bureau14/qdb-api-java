import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import net.quasardb.qdb.*;

public class Helpers {
    private static QdbCluster cluster = createCluster();
    private static long n = 1;
    public static final String RESERVED_ALIAS = "\u0000 is serialized as C0 80";
    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    public static final QdbCluster.SecurityOptions SECURITRY_OPTIONS =
        new QdbCluster.SecurityOptions("qdb-api-python",
                                       "SoHHpH26NtZvfq5pqm/8BXKbVIkf+yYiVZ5fQbq1nbcI=",
                                       "Pb+d1o3HuFtxEb5uTl9peU89ze9BZTK9f8KdKr4k7zGA=");

    public static QdbCluster createCluster() {
        try {
            return new QdbCluster(DaemonRunner.uri());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static QdbCluster createSecureCluster() {
        try {
            return new QdbCluster(DaemonRunner.secureUri(),
                                  SECURITRY_OPTIONS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
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

    public static <T extends Serializable> byte[] serialize(T obj)
        throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        return baos.toByteArray();
    }

    public static <T extends Serializable> T deserialize(byte[] b, Class<T> cl)
        throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(b);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object o = ois.readObject();
        return cl.cast(o);
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
        return UUID.randomUUID().toString();
    }

    public static QdbBlob createEmptyBlob() {
        return getBlob(createUniqueAlias());
    }

    public static QdbBlob createBlob() {
        QdbBlob blob = createEmptyBlob();
        blob.put(createSampleData());
        return blob;
    }

    public static QdbDeque createDeque() {
        QdbDeque deque = createEmptyDeque();
        deque.pushBack(createSampleData());
        return deque;
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

    public static QdbTag createEmptyTag() {
        return getTag(createUniqueAlias());
    }

    public static QdbHashSet createHashSet() {
        QdbHashSet hset = createEmptyHashSet();
        hset.insert(createSampleData());
        return hset;
    }

    public static QdbInteger createInteger() {
        QdbInteger integer = createEmptyInteger();
        integer.put(42);
        return integer;
    }

    public static QdbStream createStream() throws IOException {
        QdbStream stream = createEmptyStream();
        try (SeekableByteChannel channel = stream.open(QdbStream.Mode.APPEND)) {
            channel.write(createSampleData());
        }
        return stream;
    }

    public static QdbTag createTag() {
        QdbBlob blob = createBlob();
        QdbTag tag = createEmptyTag();
        blob.attachTag(tag.alias());
        return tag;
    }

    public static QdbTimeSeries createTimeSeries(Collection<QdbColumnDefinition> columns) throws IOException {
        return cluster.createTimeSeries(createUniqueAlias(), 86400000, columns);
    }

    public static QdbTimeSeries getTimeSeries(String alias) throws IOException {
        return cluster.timeSeries(alias);
    }

    public static QdbBlobColumnCollection createBlobColumnCollection(String alias) {
        return createBlobColumnCollection(alias, 100);
    }

    public static QdbBlobColumnCollection createBlobColumnCollection(String alias, int max) {
        QdbBlobColumnCollection v = new QdbBlobColumnCollection(alias);

        for (int i = 0; i < max; ++i) {
            v.add(new QdbBlobColumnValue(createSampleData()));
        }

        return v;
    }

    public static QdbDoubleColumnCollection createDoubleColumnCollection(String alias) {
        return createDoubleColumnCollection(alias, 10000);
    }

    public static double randomDouble() {
        return new Random(n++).nextDouble();
    }

    public static QdbDoubleColumnCollection createDoubleColumnCollection(String alias, int max) {
        QdbDoubleColumnCollection v = new QdbDoubleColumnCollection(alias);
        for (int i = 0; i < max; ++i) {
            v.add(new QdbDoubleColumnValue(randomDouble()));
        }

        return v;
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

    public static QdbEntry getEntry(String alias) {
        return cluster.entry(alias);
    }

    public static QdbHashSet getHashSet(String alias) {
        return cluster.hashSet(alias);
    }

    public static QdbInteger getInteger(String alias) {
        return cluster.integer(alias);
    }

    public static QdbNode getNode() {
        return cluster.node(DaemonRunner.uri());
    }

    public static QdbStream getStream(String alias) {
        return cluster.stream(alias);
    }

    public static QdbTag getTag(String alias) {
        return cluster.tag(alias);
    }

    public static <T> List<T> toList(Iterable<T> source) {
        List<T> list = new ArrayList<T>();
        for (T item : source)
            list.add(item);
        return list;
    }

    public static boolean looksLikeJson(String str) {
        return str.startsWith("{") && str.endsWith("}") && str.length() > 10;
    }
}
