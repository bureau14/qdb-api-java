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

    @Test
    public void listsColumns_afterCreation() throws Exception {
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbTimeSeries.DoubleColumnDefinition ("d1"),
                                                   new QdbTimeSeries.BlobColumnDefinition ("b1")));

        Iterable<QdbTimeSeries.ColumnDefinition> result = series.listColumns();
        List<QdbTimeSeries.ColumnDefinition> resultAsList = Helpers.toList(result);

        System.out.println("results = " + resultAsList.toString());


    }
}
