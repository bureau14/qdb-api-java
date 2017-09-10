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
        System.out.println("data0 = " + data.toString());

        series.insertDoubles(data);
    }

    @Test
    public void canGetResults_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));

        QdbDoubleColumnCollection data = Helpers.createDoubleColumnCollection(alias);

        System.out.println("data1 = " + data.toString());

        QdbTimeRange dataRange = data.range();
        series.insertDoubles(data);

        QdbTimeRangeCollection ranges = new QdbTimeRangeCollection();
        ranges.add(new QdbTimeRange(dataRange.getBegin(),
                                    new QdbTimespec(dataRange.getEnd().getValue().plusNanos(1))));

        QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);

        assertThat(results, (is(data)));
    }

    @Test
    public void canGetAgregates_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));


        QdbDoubleColumnCollection data = new QdbDoubleColumnCollection(alias);
        data.add(new QdbDoubleColumnValue(1.00));
        data.add(new QdbDoubleColumnValue(2.00));

        System.out.println("data2 = " + data.toString());

        QdbTimeRange dataRange = data.range();
        series.insertDoubles(data);

        QdbDoubleAggregationCollection aggregations = new QdbDoubleAggregationCollection();
        aggregations.add(new QdbDoubleAggregation(QdbAggregation.Type.FIRST,
                                                  new QdbTimeRange(dataRange.getBegin(),
                                                                   new QdbTimespec(dataRange.getEnd().getValue().plusNanos(1)))));

        QdbDoubleAggregationCollection result = series.doubleAggregate(alias, aggregations);
        assertEquals(result.size(), aggregations.size());

        //QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);

        //assertThat(results, (is(data)));

    }
}
