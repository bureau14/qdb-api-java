package net.quasardb.qdb.timeseries;

import java.nio.ByteBuffer;
import java.lang.Exception;
import java.util.*;
import java.util.stream.*;
import java.util.concurrent.*;
import org.junit.*;
import org.hamcrest.Matcher;

import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;

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
        Column[] definition = {
            new Column.Double (alias)
        };
        QdbTimeSeries series = Helpers.createTimeSeries(definition);

        QdbDoubleColumnCollection data = Helpers.createDoubleColumnCollection(alias);

        series.insertDoubles(data);
    }

    @Test
    public void canGetResults_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        Column[] definition = {
            new Column.Double (alias)
        };

        QdbTimeSeries series =
            Helpers.createTimeSeries(definition);

        QdbDoubleColumnCollection data = Helpers.createDoubleColumnCollection(alias);

        TimeRange dataRange = data.range();
        series.insertDoubles(data);

        TimeRange[] ranges = {
            new TimeRange(dataRange.getBegin(),
                             new Timespec(dataRange.getEnd().asLocalDateTime().plusNanos(1)))
        };

        QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);

        assertThat(results, (is(data)));
    }

    @Test
    public void canGetResults_afterParallelInsert() throws Exception {
        String columnAlias = Helpers.createUniqueAlias();
        Column[] definition = {
            new Column.Double(columnAlias)
        };
        QdbTimeSeries series =
            Helpers.createTimeSeries(definition);

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

        TimeRange dataRange = data.range();
        TimeRange[] ranges = {
            new TimeRange(dataRange.getBegin(),
                             new Timespec(dataRange.getEnd().asLocalDateTime().plusNanos(1)))
        };

        QdbDoubleColumnCollection results = series.getDoubles(columnAlias, ranges);
        for (QdbDoubleColumnValue expected : data) {
            assertThat(results, hasItem(expected));
        }

    }

    @Test
    public void canAgregate_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        Column[] definition = {
            new Column.Double (alias)
        };
        QdbTimeSeries series = Helpers.createTimeSeries(definition);

        QdbDoubleColumnCollection data = new QdbDoubleColumnCollection(alias);
        data.add(new QdbDoubleColumnValue(1.00));
        data.add(new QdbDoubleColumnValue(2.00));

        TimeRange dataRange = data.range();
        series.insertDoubles(data);

        QdbDoubleAggregationCollection aggregations = new QdbDoubleAggregationCollection();
        aggregations.add(new QdbDoubleAggregation(QdbAggregation.Type.FIRST,
                                                  new TimeRange(dataRange.getBegin(),
                                                                   new Timespec(dataRange.getEnd().asLocalDateTime().plusNanos(1)))));

        QdbDoubleAggregationCollection result = series.doubleAggregate(alias, aggregations);
        assertEquals(result.size(), aggregations.size());
    }

    @Test
    public void canAgregateFirst_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        Column[] definition = {
            new Column.Double (alias)
        };
        QdbTimeSeries series = Helpers.createTimeSeries(definition);


        QdbDoubleColumnCollection data = new QdbDoubleColumnCollection(alias);
        data.add(new QdbDoubleColumnValue(1.00));
        data.add(new QdbDoubleColumnValue(2.00));

        TimeRange dataRange = data.range();
        series.insertDoubles(data);

        QdbDoubleAggregationCollection aggregations = new QdbDoubleAggregationCollection();
        aggregations.add(new QdbDoubleAggregation(QdbAggregation.Type.FIRST,
                                                  new TimeRange(dataRange.getBegin(),
                                                                   new Timespec(dataRange.getEnd().asLocalDateTime().plusNanos(1)))));

        QdbDoubleAggregationCollection result = series.doubleAggregate(alias, aggregations);
        assertEquals(result.size(), aggregations.size());
        assertEquals(1.00, result.get(0).getResult().getValue().doubleValue(), 0.01);
    }


    @Test
    public void canAgregateLast_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        Column[] definition = {
            new Column.Double (alias)
        };
        QdbTimeSeries series = Helpers.createTimeSeries(definition);


        QdbDoubleColumnCollection data = new QdbDoubleColumnCollection(alias);
        data.add(new QdbDoubleColumnValue(1.00));
        data.add(new QdbDoubleColumnValue(2.00));

        TimeRange dataRange = data.range();
        series.insertDoubles(data);

        QdbDoubleAggregationCollection aggregations = new QdbDoubleAggregationCollection();
        aggregations.add(new QdbDoubleAggregation(QdbAggregation.Type.LAST,
                                                  new TimeRange(dataRange.getBegin(),
                                                                   new Timespec(dataRange.getEnd().asLocalDateTime().plusNanos(1)))));

        QdbDoubleAggregationCollection result = series.doubleAggregate(alias, aggregations);
        assertEquals(result.size(), aggregations.size());
        assertEquals(2.00, result.get(0).getResult().getValue().doubleValue(), 0.01);
    }

    @Test
    public void canAgregateCount_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        Column[] definition = {
            new Column.Double (alias)
        };
        QdbTimeSeries series = Helpers.createTimeSeries(definition);

        QdbDoubleColumnCollection data = new QdbDoubleColumnCollection(alias);
        data.add(new QdbDoubleColumnValue(1.00));
        data.add(new QdbDoubleColumnValue(2.00));

        TimeRange dataRange = data.range();
        series.insertDoubles(data);

        QdbDoubleAggregationCollection aggregations = new QdbDoubleAggregationCollection();
        aggregations.add(new QdbDoubleAggregation(QdbAggregation.Type.COUNT,
                                                  new TimeRange(dataRange.getBegin(),
                                                                new Timespec(dataRange.getEnd().asLocalDateTime().plusNanos(1)))));

        QdbDoubleAggregationCollection result = series.doubleAggregate(alias, aggregations);
        assertEquals(result.size(), aggregations.size());
        assertEquals(2, result.get(0).getCount());
    }
}
