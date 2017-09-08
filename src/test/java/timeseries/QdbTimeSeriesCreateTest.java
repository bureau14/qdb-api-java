import java.nio.ByteBuffer;
import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbTimeSeriesCreateTest {
    @Test
    public void doesNotThrow_afterCreation() throws Exception {
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbTimeSeries.DoubleColumnDefinition ("d1"),
                                                   new QdbTimeSeries.BlobColumnDefinition ("b1")));
    }
}
