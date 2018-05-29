import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.*;
import java.util.concurrent.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.IncompatibleTypeException;

public class ValueTest {

    @Test
    public void canCreateInt64() throws Exception {
        long l = Helpers.randomInt64();
        Value value = Value.createInt64(l);

        assertThat(l, equalTo(value.getInt64()));
    }

    @Test
    public void canCreateDouble() throws Exception {
        double d = Helpers.randomDouble();
        Value value = Value.createDouble(d);
        assertThat(d, equalTo(value.getDouble()));
    }

    @Test
    public void canCreateTimestamp() throws Exception {
        Timespec t = Helpers.randomTimestamp();
        Value value = Value.createTimestamp(t);

        assertThat(t, equalTo(value.getTimestamp()));
    }

    @Test
    public void canCreateBlob() throws Exception {
        ByteBuffer b = Helpers.createSampleData();
        Value value = Value.createBlob(b);

        assertThat(b, equalTo(value.getBlob()));
    }

    @Test
    public void canCreateSafeBlob() throws Exception {
        Value value1 =
            Value.createSafeBlob(Helpers.createSampleData());
        Value value2 =
            Value.createSafeBlob(value1.getBlob());

        assertThat(value1, equalTo(value2));
    }

    @Test(expected = IncompatibleTypeException.class)
    public void throwsError_whenDoubleTypesDontMatch() throws Exception {
        ByteBuffer b = Helpers.createSampleData();
        Value value = Value.createBlob(b);
        value.getDouble(); // <-- throws
    }

    @Test(expected = IncompatibleTypeException.class)
    public void throwsError_whenBlobTypesDontMatch() throws Exception {
        double d = Helpers.randomDouble();
        Value value = Value.createDouble(d);
        value.getBlob(); // <-- throws
    }

    @Test
    public void canCompareInt64s() throws Exception {
        Value value1 = Value.createInt64(Helpers.randomInt64());
        Value value2 = Value.createInt64(Helpers.randomInt64());
        Value value3 = Value.createInt64(value1.getInt64());

        assertThat(value1, not(equalTo(value2)));
        assertThat(value1, equalTo(value3));
        assertThat(value2, not(equalTo(value3)));
    }

    @Test
    public void canCompareDoubles() throws Exception {
        Value value1 = Value.createDouble(Helpers.randomDouble());
        Value value2 = Value.createDouble(Helpers.randomDouble());
        Value value3 = Value.createDouble(value1.getDouble());

        assertThat(value1, not(equalTo(value2)));
        assertThat(value1, equalTo(value3));
        assertThat(value2, not(equalTo(value3)));
    }

    @Test
    public void canCompareTimestamps() throws Exception {
        Value value1 = Value.createTimestamp(Helpers.randomTimestamp());
        Value value2 = Value.createTimestamp(Helpers.randomTimestamp());
        Value value3 = Value.createTimestamp(value1.getTimestamp());

        assertThat(value1, not(equalTo(value2)));
        assertThat(value1, equalTo(value3));
        assertThat(value2, not(equalTo(value3)));
    }

    @Test
    public void canCompareBlobs() throws Exception {
        ByteBuffer b1 = Helpers.createSampleData();
        ByteBuffer b2 = Helpers.createSampleData();
        Value value1 = Value.createBlob(b1);
        Value value2 = Value.createBlob(b2);
        Value value3 = Value.createBlob(b1);

        assertThat(value1, not(equalTo(value2)));
        assertThat(value1, equalTo(value3));
        assertThat(value2, not(equalTo(value3)));
    }

    @Test
    public void canCompareBlobAndDoubles() throws Exception {
        ByteBuffer b = Helpers.createSampleData();
        Double d = Helpers.randomDouble();
        Value value1 = Value.createBlob(b);
        Value value2 = Value.createDouble(d);

        assertThat(value1, not(equalTo(value2)));
    }

    @Test
    public void canSerialize_andDeserialize_Int64s() throws Exception {
        Value vBefore = Value.createInt64(Helpers.randomInt64());

        byte[] serialized = Helpers.serialize(vBefore);
        Value vAfter = (Value)Helpers.deserialize(serialized, vBefore.getClass());

        assertThat(vBefore, equalTo(vAfter));
    }

    @Test
    public void canSerialize_andDeserialize_Doubles() throws Exception {
        Value vBefore = Value.createDouble(Helpers.randomDouble());

        byte[] serialized = Helpers.serialize(vBefore);
        Value vAfter = (Value)Helpers.deserialize(serialized, vBefore.getClass());

        assertThat(vBefore, equalTo(vAfter));
    }

    @Test
    public void canSerialize_andDeserialize_Timestamps() throws Exception {
        Value vBefore = Value.createTimestamp(Helpers.randomTimestamp());

        byte[] serialized = Helpers.serialize(vBefore);
        Value vAfter = (Value)Helpers.deserialize(serialized, vBefore.getClass());

        assertThat(vBefore, equalTo(vAfter));
    }

    @Test
    public void canSerialize_andDeserialize_Blobs() throws Exception {
        Value vBefore = Value.createBlob(Helpers.createSampleData());

        byte[] serialized = Helpers.serialize(vBefore);
        Value vAfter = (Value)Helpers.deserialize(serialized, vBefore.getClass());

        assertThat(vBefore, equalTo(vAfter));
    }
}
