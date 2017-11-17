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
}
