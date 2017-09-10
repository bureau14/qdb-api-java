import java.nio.ByteBuffer;
import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class QdbTimeSeriesDoubleTest {
    @Test
    public void doesNotThrow_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));

        QdbDoubleColumnCollection data = Helpers.createDoubleColumnCollection(alias);
        series.insertDoubles(data);
    }

    @Test
    public void canGetResults_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));

        QdbDoubleColumnCollection data = Helpers.createDoubleColumnCollection(alias);
        QdbTimeRange dataRange = data.range();
        series.insertDoubles(data);

        QdbTimeRangeCollection ranges = new QdbTimeRangeCollection();
        ranges.add(new QdbTimeRange(dataRange.getBegin(),
                                    new QdbTimespec(dataRange.getEnd().getValue().plusNanos(1))));

        QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);
        assertThat(results, (is(data)));

    }
}
