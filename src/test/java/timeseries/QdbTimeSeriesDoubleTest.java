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
    public void canAgregate_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));


        QdbDoubleColumnCollection data = new QdbDoubleColumnCollection(alias);
        data.add(new QdbDoubleColumnValue(1.00));
        data.add(new QdbDoubleColumnValue(2.00));

        QdbTimeRange dataRange = data.range();
        series.insertDoubles(data);

        QdbDoubleAggregationCollection aggregations = new QdbDoubleAggregationCollection();
        aggregations.add(new QdbDoubleAggregation(QdbAggregation.Type.FIRST,
                                                  new QdbTimeRange(dataRange.getBegin(),
                                                                   new QdbTimespec(dataRange.getEnd().getValue().plusNanos(1)))));

        QdbDoubleAggregationCollection result = series.doubleAggregate(alias, aggregations);
        assertEquals(result.size(), aggregations.size());
    }

    @Test
    public void canAgregateFirst_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));


        QdbDoubleColumnCollection data = new QdbDoubleColumnCollection(alias);
        data.add(new QdbDoubleColumnValue(1.00));
        data.add(new QdbDoubleColumnValue(2.00));

        QdbTimeRange dataRange = data.range();
        series.insertDoubles(data);

        QdbDoubleAggregationCollection aggregations = new QdbDoubleAggregationCollection();
        aggregations.add(new QdbDoubleAggregation(QdbAggregation.Type.FIRST,
                                                  new QdbTimeRange(dataRange.getBegin(),
                                                                   new QdbTimespec(dataRange.getEnd().getValue().plusNanos(1)))));

        QdbDoubleAggregationCollection result = series.doubleAggregate(alias, aggregations);
        assertEquals(result.size(), aggregations.size());
        assertEquals(1.00, result.get(0).getResult().getValue().doubleValue(), 0.01);
    }


    @Test
    public void canAgregateLast_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));


        QdbDoubleColumnCollection data = new QdbDoubleColumnCollection(alias);
        data.add(new QdbDoubleColumnValue(1.00));
        data.add(new QdbDoubleColumnValue(2.00));

        QdbTimeRange dataRange = data.range();
        series.insertDoubles(data);

        QdbDoubleAggregationCollection aggregations = new QdbDoubleAggregationCollection();
        aggregations.add(new QdbDoubleAggregation(QdbAggregation.Type.LAST,
                                                  new QdbTimeRange(dataRange.getBegin(),
                                                                   new QdbTimespec(dataRange.getEnd().getValue().plusNanos(1)))));

        QdbDoubleAggregationCollection result = series.doubleAggregate(alias, aggregations);
        assertEquals(result.size(), aggregations.size());
        assertEquals(2.00, result.get(0).getResult().getValue().doubleValue(), 0.01);
    }

    @Test
    public void canAgregateCount_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));


        QdbDoubleColumnCollection data = new QdbDoubleColumnCollection(alias);
        data.add(new QdbDoubleColumnValue(1.00));
        data.add(new QdbDoubleColumnValue(2.00));

        QdbTimeRange dataRange = data.range();
        series.insertDoubles(data);

        QdbDoubleAggregationCollection aggregations = new QdbDoubleAggregationCollection();
        aggregations.add(new QdbDoubleAggregation(QdbAggregation.Type.COUNT,
                                                  new QdbTimeRange(dataRange.getBegin(),
                                                                   new QdbTimespec(dataRange.getEnd().getValue().plusNanos(1)))));

        QdbDoubleAggregationCollection result = series.doubleAggregate(alias, aggregations);
        assertEquals(result.size(), aggregations.size());
        assertEquals(2, result.get(0).getCount());
    }
}
