import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.*;
import java.util.concurrent.*;
import net.quasardb.qdb.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class QdbTimeSeriesValueTest {

    @Test
    public void canCreateDouble() throws Exception {
        double d = Helpers.randomDouble();
        QdbTimeSeriesValue value = QdbTimeSeriesValue.createDouble(d);
        assertThat(d, equalTo(value.getDouble()));
    }

    @Test
    public void canCreateBlob() throws Exception {
        ByteBuffer b = Helpers.createSampleData();
        QdbTimeSeriesValue value = QdbTimeSeriesValue.createBlob(b);

        assertThat(b, equalTo(value.getBlob()));
    }

    @Test
    public void canCreateSafeBlob() throws Exception {
        QdbTimeSeriesValue value1 =
            QdbTimeSeriesValue.createSafeBlob(Helpers.createSampleData());
        QdbTimeSeriesValue value2 =
            QdbTimeSeriesValue.createSafeBlob(value1.getBlob());

        assertThat(value1, equalTo(value2));
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsError_whenDoubleTypesDontMatch() throws Exception {
        ByteBuffer b = Helpers.createSampleData();
        QdbTimeSeriesValue value = QdbTimeSeriesValue.createBlob(b);
        value.getDouble(); // <-- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsError_whenBlobTypesDontMatch() throws Exception {
        double d = Helpers.randomDouble();
        QdbTimeSeriesValue value = QdbTimeSeriesValue.createDouble(d);
        value.getBlob(); // <-- throws
    }

    @Test
    public void canCompareDoubles() throws Exception {
        QdbTimeSeriesValue value1 = QdbTimeSeriesValue.createDouble(Helpers.randomDouble());
        QdbTimeSeriesValue value2 = QdbTimeSeriesValue.createDouble(Helpers.randomDouble());
        QdbTimeSeriesValue value3 = QdbTimeSeriesValue.createDouble(value1.getDouble());

        assertThat(value1, not(equalTo(value2)));
        assertThat(value1, equalTo(value3));
        assertThat(value2, not(equalTo(value3)));
    }

    @Test
    public void canCompareBlobs() throws Exception {
        ByteBuffer b1 = Helpers.createSampleData();
        ByteBuffer b2 = Helpers.createSampleData();
        QdbTimeSeriesValue value1 = QdbTimeSeriesValue.createBlob(b1);
        QdbTimeSeriesValue value2 = QdbTimeSeriesValue.createBlob(b2);
        QdbTimeSeriesValue value3 = QdbTimeSeriesValue.createBlob(b1);

        assertThat(value1, not(equalTo(value2)));
        assertThat(value1, equalTo(value3));
        assertThat(value2, not(equalTo(value3)));
    }

    @Test
    public void canCompareBlobAndDoubles() throws Exception {
        ByteBuffer b = Helpers.createSampleData();
        Double d = Helpers.randomDouble();
        QdbTimeSeriesValue value1 = QdbTimeSeriesValue.createBlob(b);
        QdbTimeSeriesValue value2 = QdbTimeSeriesValue.createDouble(d);

        assertThat(value1, not(equalTo(value2)));
    }

    @Test
    public void canSerialize_andDeserialize_Doubles() throws Exception {
        double d = Helpers.randomDouble();
        QdbTimeSeriesValue vBefore = QdbTimeSeriesValue.createDouble(d);

        byte[] serialized = Helpers.serialize(vBefore);
        QdbTimeSeriesValue vAfter = (QdbTimeSeriesValue)Helpers.deserialize(serialized, vBefore.getClass());

        assertThat(vBefore, equalTo(vAfter));
    }

    @Test
    public void canSerialize_andDeserialize_Blobs() throws Exception {
        QdbTimeSeriesValue vBefore = QdbTimeSeriesValue.createBlob(Helpers.createSampleData());

        byte[] serialized = Helpers.serialize(vBefore);
        QdbTimeSeriesValue vAfter = (QdbTimeSeriesValue)Helpers.deserialize(serialized, vBefore.getClass());

        assertThat(vBefore, equalTo(vAfter));
    }
}
