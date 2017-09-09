import java.nio.ByteBuffer;
import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbTimeSeriesCreateTest {
    @Test
    public void doesNotThrow_afterCreation() throws Exception {
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(QdbTimeSeries.ColumnDefinition.createDouble ("d1"),
                                                   QdbTimeSeries.ColumnDefinition.createBlob ("b1")));
    }

    @Test
    public void listsColumns_afterCreation() throws Exception {
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(QdbTimeSeries.ColumnDefinition.createDouble ("d1"),
                                                   QdbTimeSeries.ColumnDefinition.createBlob ("b1")));

        Iterable<QdbTimeSeries.ColumnDefinition> result = series.listColumns();
        List<QdbTimeSeries.ColumnDefinition> resultAsList = Helpers.toList(result);


        Assert.assertEquals(resultAsList.size(), 2);

        List<String> strings = new ArrayList<String> ();
    }
}
