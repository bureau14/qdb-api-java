package net.quasardb.qdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.Serializable;

import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.stream.Stream;
import java.util.function.Supplier;

import org.json.JSONObject;

import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;

public class Helpers {
    private static QdbCluster cluster = createCluster();
    private static long n = 1;
    public static final String RESERVED_ALIAS = "\u0000 is serialized as C0 80";
    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    public static Session.SecurityOptions get_secure_options() {
        String username = "";
        String user_secret_key = "";
        String cluster_public_key = "";
        try {
            String user_file_content = new String(Files.readAllBytes(Paths.get("user_private.key")));
            JSONObject user = new JSONObject(user_file_content);
            username = user.get("username").toString();
            user_secret_key = user.get("secret_key").toString();
            cluster_public_key = new String(Files.readAllBytes(Paths.get("cluster_public.key")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new Session.SecurityOptions(username, user_secret_key, cluster_public_key);
    }

    public static final Session.SecurityOptions SECURITRY_OPTIONS = get_secure_options();

    public static QdbCluster createCluster() {
        QdbCluster cluster = new QdbCluster(Daemon.uri());
        cluster.waitForStabilization(60000);
        return cluster;
    }

    public static QdbCluster createSecureCluster() {
        return new QdbCluster(Daemon.secureUri(),
                              SECURITRY_OPTIONS);
    }

    public static Session getSession() {
        return cluster.getSession();
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


    public static Column[] generateTableColumns(int count) {
        return generateTableColumns(Value.Type.DOUBLE, count);
    }

    public static Column[] generateTableColumns(Value.Type valueType, int count) {
        return Stream.generate(Helpers::createUniqueAlias)
            .limit(count)
            .map((alias) -> {
                    return new Column(alias, valueType);
                })
            .toArray(Column[]::new);
    }

    public static Value generateRandomValueByType(int complexity, Value.Type valueType) {
        switch (valueType) {
        case INT64:
            return Value.createInt64(randomInt64());
        case DOUBLE:
            return Value.createDouble(randomDouble());
        case TIMESTAMP:
            return Value.createTimestamp(randomTimestamp());
        case BLOB:
            return Value.createSafeBlob(createSampleData(complexity));
        case STRING:
            return Value.createString(randomString(complexity));
        }

        return Value.createNull();

    }

    /**
     * Generate table rows with standard complexity of 32
     */
    public static WritableRow[] generateTableRows(Column[] cols, int count) {
        return generateTableRows(cols, 32, count);
    }

    /**
     * Generate table rows.
     *
     * @param cols       Describes the table layout
     * @param complexity Arbitrary complexity variable that is used when generating data. E.g. for blobs,
     *                   this denotes the size of the blob value being generated.
     */
    public static WritableRow[] generateTableRows(Column[] cols, int complexity, int count) {
        // Generate that returns entire rows with an appropriate value for each column.
        Supplier<Value[]> valueGen =
            (() ->
             Arrays.stream(cols)
             .map(Column::getType)
             .map((Value.Type valueType) -> {
                     return Helpers.generateRandomValueByType(complexity, valueType);
                 })
             .toArray(Value[]::new));


        return Stream.generate(valueGen)
            .limit(count)
            .map((v) ->
                 new WritableRow(Timespec.now(),
                                 v))
            .toArray(WritableRow[]::new);
    }

    public static QdbTimeSeries seedTable(Column[] cols, WritableRow[] rows) throws Exception {
        return seedTable(createUniqueAlias(), cols, rows);
    }

    public static QdbTimeSeries seedTable(String tableName, Column[] cols, WritableRow[] rows) throws Exception {
        QdbTimeSeries series = createTimeSeries(tableName, cols);
        Writer writer = series.tableWriter();


        for (WritableRow row : rows) {
            writer.append(row);
        }

        writer.flush();

        return series;
    }

    /**
     * Generates a TimeRange from an array of rows. Assumes that all rows are sorted,
     * with the oldest row being first.
     */
    public static TimeRange rangeFromRows(WritableRow[] rows) {
        assert(rows.length >= 1);

        Timespec first = rows[0].getTimestamp();
        Timespec last = rows[(rows.length - 1)].getTimestamp();

        return new TimeRange(first,
                             last.plusNanos(1));
    }

    /**
     * Generates an array of TimeRange from an array of rows. Generates exactly one timerange
     * per row.
     */
    public static TimeRange[] rangesFromRows(WritableRow[] rows) {
        return Arrays.stream(rows)
            .map(WritableRow::getTimestamp)
            .map((t) -> {
                     return new TimeRange(t, t.plusNanos(1));
                })
            .toArray(TimeRange[]::new);
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

    public static ByteBuffer createSampleData() {
        return createSampleData(32);
    }

    public static ByteBuffer createSampleData(int size) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        createSampleData(size, buffer);
        return buffer;
    }

    public static void createSampleData(int size, ByteBuffer buffer) {
        byte[] b = new byte[size];
        createSampleData(b);

        buffer.put(b);
        buffer.flip();
    }

    public static void createSampleData(byte[] b) {
        new Random(n++).nextBytes(b);
    }


    public static String randomString() {
        return randomString(32);
    }

    public static String randomString(int complexity) {
        return createUniqueAlias();
    }


    public static QdbBatch createBatch() {
        return cluster.createBatch();
    }

    public static String createUniqueAlias() {
        return new String("a") + UUID.randomUUID().toString().replaceAll("-", "");


    }

    public static QdbBlob createEmptyBlob() {
        return getBlob(createUniqueAlias());
    }

    public static QdbBlob createBlob() {
        QdbBlob blob = createEmptyBlob();
        blob.put(createSampleData());
        return blob;
    }

    public static QdbInteger createEmptyInteger() {
        return getInteger(createUniqueAlias());
    }

    public static QdbTag createEmptyTag() {
        return getTag(createUniqueAlias());
    }

    public static QdbInteger createInteger() {
        QdbInteger integer = createEmptyInteger();
        integer.put(42);
        return integer;
    }

    public static QdbTag createTag() {
        QdbBlob blob = createBlob();
        QdbTag tag = createEmptyTag();
        blob.attachTag(tag.alias());
        return tag;
    }

    public static Table createTable(Column[] columns) throws IOException {
        return Table.create(getSession(), createUniqueAlias(), columns);
    }

    public static Table createTable(Column[] columns, long shardSize) throws IOException {
        return Table.create(getSession(), createUniqueAlias(), columns, shardSize);
    }

    public static QdbTimeSeries createTimeSeries(Column[] columns) throws IOException {
        return createTimeSeries(createUniqueAlias(), columns);
    }

    public static QdbTimeSeries createTimeSeries(String alias, Column[] columns) throws IOException {
        return cluster.createTimeSeries(alias, 86400000, columns);
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

    public static long randomInt64() {
        return new Random(n++).nextLong();
    }

    public static Timespec randomTimespec() {
        return randomTimestamp();

    }

    public static Timespec randomTimestamp() {
        return new Timespec(new Random(n++).nextInt(),
                            new Random(n++).nextInt());

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

    public static QdbEntry getEntry(String alias) {
        return cluster.entry(alias);
    }

    public static QdbInteger getInteger(String alias) {
        return cluster.integer(alias);
    }

    public static QdbNode getNode() {
        return cluster.node(Daemon.uri());
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
