import java.nio.ByteBuffer;
import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbTimeSeriesCreateTest {
    @Test
    public void doesNotThrow_afterCreation() throws Exception {
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumn.Definition.Double ("d1"),
                                                   new QdbColumn.Definition.Blob ("b1")));
    }

    @Test
    public void listsColumns_afterCreation() throws Exception {
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumn.Definition.Double ("d1"),
                                                   new QdbColumn.Definition.Blob ("b1")));

        Iterable<QdbColumn.Definition> result = series.listColumns();
        List<QdbColumn.Definition> resultAsList = Helpers.toList(result);

        Assert.assertEquals(resultAsList.size(), 2);

        System.out.println("columns = " + resultAsList.toString());
    }
}
