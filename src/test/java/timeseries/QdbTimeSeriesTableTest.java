import java.util.*;
import java.time.*;
import net.quasardb.qdb.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class QdbTimeSeriesTableTest {

    @Test
    public void canGetTable() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeriesTable table =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).table();
    }

    @Test
    public void canFlushTable() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeriesTable table =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).table();

        table.flush();
    }

    @Test
    public void canLookupColumnOffsetById() throws Exception {
        QdbColumnDefinition[] columns = {
            new QdbColumnDefinition.Double(Helpers.createUniqueAlias()),
            new QdbColumnDefinition.Double(Helpers.createUniqueAlias()),
            new QdbColumnDefinition.Double(Helpers.createUniqueAlias()),
            new QdbColumnDefinition.Double(Helpers.createUniqueAlias()),
            new QdbColumnDefinition.Double(Helpers.createUniqueAlias()),
            new QdbColumnDefinition.Double(Helpers.createUniqueAlias()),
            new QdbColumnDefinition.Double(Helpers.createUniqueAlias()),
            new QdbColumnDefinition.Double(Helpers.createUniqueAlias())
        };

        QdbTimeSeriesTable table =
            Helpers.createTimeSeries(Arrays.asList(columns)).table();

        for (int i = 0; i < columns.length; ++i) {
            QdbColumnDefinition column = columns[i];
            assertThat(table.columnIndexById(column.getName()), equalTo(i));
        }
    }


    @Test
    public void canCloseTable() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeriesTable table =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).table();

        table.close();
    }

    @Test
    public void canInsertDoubleRow() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));
        QdbTimeSeriesTable table = series.table();

        QdbTimeSeriesValue[] values = {
            QdbTimeSeriesValue.createDouble(Helpers.randomDouble())
        };

        QdbTimespec timestamp = new QdbTimespec(LocalDateTime.now());
        QdbTimeSeriesRow row = new QdbTimeSeriesRow(timestamp,
                                                    values);
        table.append(row);
        table.flush();

        QdbTimeRangeCollection ranges = new QdbTimeRangeCollection();
        ranges.add(new QdbTimeRange(timestamp,
                                    new QdbTimespec(timestamp.asLocalDateTime().plusNanos(1))));

        QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);

        assertThat(results.size(), (is(1)));
        assertThat(results.get(0).getValue(), equalTo(values[0].getDouble()));
    }

    @Test
    public void canInsertMultipleDoubleRows() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));
        QdbTimeSeriesTable table = series.table();

        int ROW_COUNT = 100000;
        QdbTimeSeriesRow[] rows = new QdbTimeSeriesRow[ROW_COUNT];
        for (int i = 0; i < rows.length; ++i) {
            rows[i] =
                new QdbTimeSeriesRow (LocalDateTime.now(),
                                      new QdbTimeSeriesValue[] {
                                          QdbTimeSeriesValue.createDouble(Helpers.randomDouble())});
            table.append(rows[i]);
        }

        table.flush();

        QdbTimeRangeCollection ranges = new QdbTimeRangeCollection();
        ranges.add(new QdbTimeRange(rows[0].getTimestamp(),
                                    new QdbTimespec(rows[(rows.length - 1)].getTimestamp().asLocalDateTime().plusNanos(1))));

        QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);
        assertThat(results.size(), (is(rows.length)));

        for (int i = 0; i < rows.length; ++i) {
            assertThat(results.get(i).getValue(), equalTo(rows[i].getValues()[0].getDouble()));
        }

    }

    @Test
    public void canInsertBlobRow() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Blob (alias)));
        QdbTimeSeriesTable table = series.table();

        QdbTimeSeriesValue[] values = {
            QdbTimeSeriesValue.createSafeBlob(Helpers.createSampleData())
        };

        QdbTimespec timestamp = new QdbTimespec(LocalDateTime.now());
        QdbTimeSeriesRow row = new QdbTimeSeriesRow(timestamp,
                                                    values);
        table.append(row);
        table.flush();

        QdbTimeRangeCollection ranges = new QdbTimeRangeCollection();
        ranges.add(new QdbTimeRange(timestamp,
                                    new QdbTimespec(timestamp.asLocalDateTime().plusNanos(1))));

        QdbBlobColumnCollection results = series.getBlobs(alias, ranges);

        assertThat(results.size(), (is(1)));
        assertThat(results.get(0).getValue(), equalTo(values[0].getBlob()));
    }

    @Test
    public void canInsertMultipleColumns() throws Exception {
        String alias1 = Helpers.createUniqueAlias();
        String alias2 = Helpers.createUniqueAlias();

        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias1),
                                                                      new QdbColumnDefinition.Blob (alias2)));
        QdbTimeSeriesTable table = series.table();

        QdbTimeSeriesValue[] values = {
            QdbTimeSeriesValue.createDouble(Helpers.randomDouble()),
            QdbTimeSeriesValue.createBlob(Helpers.createSampleData())
        };

        QdbTimespec timestamp = new QdbTimespec(LocalDateTime.now());
        QdbTimeSeriesRow row = new QdbTimeSeriesRow(timestamp,
                                                    values);

        table.append(row);
        table.flush();

        QdbTimeRangeCollection ranges = new QdbTimeRangeCollection();
        ranges.add(new QdbTimeRange(timestamp,
                                    new QdbTimespec(timestamp.asLocalDateTime().plusNanos(1))));

        QdbDoubleColumnCollection results1 = series.getDoubles(alias1, ranges);
        QdbBlobColumnCollection results2 = series.getBlobs(alias2, ranges);

        assertThat(results1.size(), (is(1)));
        assertThat(results2.size(), (is(1)));
        assertThat(results1.get(0).getValue(), equalTo(values[0].getDouble()));
        assertThat(results2.get(0).getValue(), equalTo(values[1].getBlob()));
    }

    @Test
    public void canInsertNullColumns() throws Exception {
        String alias1 = Helpers.createUniqueAlias();
        String alias2 = Helpers.createUniqueAlias();

        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias1),
                                                                      new QdbColumnDefinition.Blob (alias2)));
        QdbTimeSeriesTable table = series.table();

        QdbTimeSeriesValue[] values = {
            QdbTimeSeriesValue.createDouble(Helpers.randomDouble()),
            QdbTimeSeriesValue.createNull()
        };

        QdbTimespec timestamp = new QdbTimespec(LocalDateTime.now());
        QdbTimeSeriesRow row = new QdbTimeSeriesRow(timestamp,
                                                    values);

        table.append(row);
        table.flush();

        QdbTimeRangeCollection ranges = new QdbTimeRangeCollection();
        ranges.add(new QdbTimeRange(timestamp,
                                    new QdbTimespec(timestamp.asLocalDateTime().plusNanos(1))));

        QdbDoubleColumnCollection results1 = series.getDoubles(alias1, ranges);
        QdbBlobColumnCollection results2 = series.getBlobs(alias2, ranges);

        assertThat(results1.size(), (is(1)));
        assertThat(results2.size(), (is(0)));
        assertThat(results1.get(0).getValue(), equalTo(values[0].getDouble()));
    }

    @Test
    public void tableIsFlushed_whenClosed() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));
        QdbTimeSeriesTable table = series.table();

        QdbTimeSeriesValue[] values = {
            QdbTimeSeriesValue.createDouble(Helpers.randomDouble())
        };

        QdbTimespec timestamp = new QdbTimespec(LocalDateTime.now());
        QdbTimeSeriesRow row = new QdbTimeSeriesRow(timestamp,
                                                    values);
        table.append(row);
        table.close();

        QdbTimeRangeCollection ranges = new QdbTimeRangeCollection();
        ranges.add(new QdbTimeRange(timestamp,
                                    new QdbTimespec(timestamp.asLocalDateTime().plusNanos(1))));

        QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);

        assertThat(results.size(), (is(1)));
        assertThat(results.get(0).getValue(), equalTo(values[0].getDouble()));
    }

    @Test
    public void autoFlushTable_isFlushed_whenThresholdReached() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));
        QdbTimeSeriesTable table = series.autoFlushTable(2); // flush every 2 rows

        QdbTimeSeriesValue[] values = {
            QdbTimeSeriesValue.createDouble(Helpers.randomDouble())
        };

        QdbTimespec timestamp = new QdbTimespec(LocalDateTime.now());
        QdbTimeSeriesRow row = new QdbTimeSeriesRow(timestamp,
                                                    values);
        table.append(row);

        QdbTimeRangeCollection ranges = new QdbTimeRangeCollection();
        ranges.add(new QdbTimeRange(timestamp,
                                    new QdbTimespec(timestamp.asLocalDateTime().plusNanos(1))));

        QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);

        assertThat(results.size(), (is(0)));

        // Add another row, which should trigger flush
        table.append(row);

        results = series.getDoubles(alias, ranges);

        assertThat(results.size(), (is(2)));
        assertThat(results.get(0).getValue(), equalTo(values[0].getDouble()));
        assertThat(results.get(1).getValue(), equalTo(values[0].getDouble()));
    }

}
