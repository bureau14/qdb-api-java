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
        QdbTimeSeriesValue value = new QdbTimeSeriesValue.Double(d);
        assertEquals(d, value.getDouble(), 0.01);
    }

    @Test
    public void canCreateBlob() throws Exception {
        ByteBuffer b = Helpers.createSampleData();
        QdbTimeSeriesValue value = new QdbTimeSeriesValue.Blob(b);
        assertEquals(b, value.getBlob());
    }

    @Test
    public void canCreateSafeBlob() throws Exception {
        QdbTimeSeriesValue value1 =
            new QdbTimeSeriesValue.SafeBlob(Helpers.createSampleData());
        QdbTimeSeriesValue value2 =
            new QdbTimeSeriesValue.SafeBlob(value1.getBlob());

        assertEquals(value1, value2);
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsError_whenDoubleTypesDontMatch() throws Exception {
        ByteBuffer b = Helpers.createSampleData();
        QdbTimeSeriesValue value = new QdbTimeSeriesValue.Blob(b);
        value.getDouble(); // <-- throws
    }

    @Test(expected = QdbIncompatibleTypeException.class)
    public void throwsError_whenBlobTypesDontMatch() throws Exception {
        double d = Helpers.randomDouble();
        QdbTimeSeriesValue value = new QdbTimeSeriesValue.Double(d);
        value.getBlob(); // <-- throws
    }

    @Test
    public void canCompareDoubles() throws Exception {
        QdbTimeSeriesValue value1 = new QdbTimeSeriesValue.Double(Helpers.randomDouble());
        QdbTimeSeriesValue value2 = new QdbTimeSeriesValue.Double(Helpers.randomDouble());
        QdbTimeSeriesValue value3 = new QdbTimeSeriesValue.Double(value1.getDouble());

        assertFalse(value1.equals(value2));
        assertEquals(value1, value3);
        assertFalse(value2.equals(value3));
    }

    @Test
    public void canCompareBlobs() throws Exception {
        ByteBuffer b1 = Helpers.createSampleData();
        ByteBuffer b2 = Helpers.createSampleData();
        QdbTimeSeriesValue value1 = new QdbTimeSeriesValue.Blob(b1);
        QdbTimeSeriesValue value2 = new QdbTimeSeriesValue.Blob(b2);
        QdbTimeSeriesValue value3 = new QdbTimeSeriesValue.Blob(b1);

        assertFalse(value1.equals(value2));
        assertEquals(value1, value3);
        assertFalse(value2.equals(value3));
    }

    @Test
    public void canCompareBlobAndDoubles() throws Exception {
        ByteBuffer b = Helpers.createSampleData();
        Double d = Helpers.randomDouble();
        QdbTimeSeriesValue value1 = new QdbTimeSeriesValue.Blob(b);
        QdbTimeSeriesValue value2 = new QdbTimeSeriesValue.Double(d);

        assertFalse(value1.equals(value2));
    }
}
