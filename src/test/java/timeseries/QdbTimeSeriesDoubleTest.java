import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.*;
import java.util.concurrent.*;
import net.quasardb.qdb.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class QdbTimeSeriesDoubleTest {

    @Test
    public void canSerialize_andDeserialize() throws Exception {
        double data = Helpers.randomDouble();
        QdbDoubleColumnValue vBefore = new QdbDoubleColumnValue(data);

        byte[] serialized = Helpers.serialize(vBefore);
        QdbDoubleColumnValue vAfter = (QdbDoubleColumnValue)Helpers.deserialize(serialized, vBefore.getClass());

        assertEquals(vBefore, vAfter);
    }

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
                                    new QdbTimespec(dataRange.getEnd().asLocalDateTime().plusNanos(1))));

        QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);

        assertThat(results, (is(data)));
    }

    @Test
    public void canGetResults_afterParallelInsert() throws Exception {
        String columnAlias = Helpers.createUniqueAlias();
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double(columnAlias)));

        String seriesAlias = series.getName();

        Callable<QdbDoubleColumnCollection> insertTask = () -> {
            Boolean success = false;
            QdbDoubleColumnCollection data = Helpers.createDoubleColumnCollection(columnAlias);

            while (success == false) {
                try {
                    QdbTimeSeries taskSeries = Helpers.getTimeSeries(seriesAlias);
                    taskSeries.insertDoubles(data);
                    success = true;
                } catch (Exception e) {
                    System.out.println("caught exception: " + e.toString());
                }
            }

            return data;
        };

        ExecutorService executor = Executors.newFixedThreadPool(8);
        Future<QdbDoubleColumnCollection> task1 = executor.submit(insertTask);
        Future<QdbDoubleColumnCollection> task2 = executor.submit(insertTask);
        Future<QdbDoubleColumnCollection> task3 = executor.submit(insertTask);
        Future<QdbDoubleColumnCollection> task4 = executor.submit(insertTask);
        Future<QdbDoubleColumnCollection> task5 = executor.submit(insertTask);
        Future<QdbDoubleColumnCollection> task6 = executor.submit(insertTask);
        Future<QdbDoubleColumnCollection> task7 = executor.submit(insertTask);
        Future<QdbDoubleColumnCollection> task8 = executor.submit(insertTask);

        QdbDoubleColumnCollection data = new QdbDoubleColumnCollection(columnAlias);
        data.addAll(task1.get());
        data.addAll(task2.get());
        data.addAll(task3.get());
        data.addAll(task4.get());
        data.addAll(task5.get());
        data.addAll(task6.get());
        data.addAll(task7.get());
        data.addAll(task8.get());

        QdbTimeRange dataRange = data.range();
        QdbTimeRangeCollection ranges = new QdbTimeRangeCollection();
        ranges.add(new QdbTimeRange(dataRange.getBegin(),
                                    new QdbTimespec(dataRange.getEnd().asLocalDateTime().plusNanos(1))));

        QdbDoubleColumnCollection results = series.getDoubles(columnAlias, ranges);
        for (QdbDoubleColumnValue expected : data) {
            assertThat(results, hasItem(expected));
        }

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
                                                                   new QdbTimespec(dataRange.getEnd().asLocalDateTime().plusNanos(1)))));

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
                                                                   new QdbTimespec(dataRange.getEnd().asLocalDateTime().plusNanos(1)))));

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
                                                                   new QdbTimespec(dataRange.getEnd().asLocalDateTime().plusNanos(1)))));

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
                                                                   new QdbTimespec(dataRange.getEnd().asLocalDateTime().plusNanos(1)))));

        QdbDoubleAggregationCollection result = series.doubleAggregate(alias, aggregations);
        assertEquals(result.size(), aggregations.size());
        assertEquals(2, result.get(0).getCount());
    }

    // @Test
    // public void benchmarkDoesntCrash() throws Exception {
    //     int COLUMN_COUNT = 50;
    //     int ROW_COUNT = 100000;
    //     QdbColumnDefinition[] aliases = new QdbColumnDefinition[COLUMN_COUNT];
    //     for (int i = 0; i < aliases.length; i++) {
    //         aliases[i] = new QdbColumnDefinition.Double(Helpers.createUniqueAlias());
    //     }


    //     QdbTimeSeries series =
    //         Helpers.createTimeSeries(Arrays.asList(aliases));
    //     QdbTimeRangeCollection ranges =
    //         new QdbTimeRangeCollection();
    //     int[] rowLengths = new int[COLUMN_COUNT];

    //     for (int i = 0; i < aliases.length; i++) {
    //         QdbColumnDefinition alias = aliases[i];

    //         QdbDoubleColumnCollection data = Helpers.createDoubleColumnCollection(alias.getName(), ROW_COUNT);
    //         rowLengths[i] = data.size();

    //         QdbTimeRange dataRange = data.range();

    //         series.insertDoubles(data);
    //         ranges.add(new QdbTimeRange(dataRange.getBegin(),
    //                                     new QdbTimespec(dataRange.getEnd().getValue().plusNanos(1))));

    //     }

    //     for (int i = 0; i < aliases.length; i++) {
    //         QdbColumnDefinition alias = aliases[i];

    //         QdbDoubleColumnCollection results = series.getDoubles(alias.getName(), ranges);
    //         assertThat(results.size(), (is(rowLengths[i])));
    //     }
    // }
}
