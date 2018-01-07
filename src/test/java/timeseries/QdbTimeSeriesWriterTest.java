import java.util.*;
import java.time.*;
import net.quasardb.qdb.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class QdbTimeSeriesWriterTest {

    @Test
    public void canGetWriter() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeriesWriter writer =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).tableWriter();
    }

    @Test
    public void canFlushWriter() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeriesWriter writer =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).tableWriter();

        writer.flush();
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

        QdbTimeSeriesWriter writer =
            Helpers.createTimeSeries(Arrays.asList(columns)).tableWriter();

        for (int i = 0; i < columns.length; ++i) {
            QdbColumnDefinition column = columns[i];
            assertThat(writer.getTable().columnIndexById(column.getName()), equalTo(i));
        }
    }


    @Test
    public void canCloseWriter() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeriesWriter writer =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).tableWriter();

        writer.close();
    }

    @Test
    public void canInsertDoubleRow() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));
        QdbTimeSeriesWriter writer = series.tableWriter();

        QdbTimeSeriesValue[] values = {
            QdbTimeSeriesValue.createDouble(Helpers.randomDouble())
        };

        QdbTimespec timestamp = new QdbTimespec(LocalDateTime.now());
        QdbTimeSeriesRow row = new QdbTimeSeriesRow(timestamp,
                                                    values);
        writer.append(row);
        writer.flush();

        QdbTimeRange[] ranges = {
            new QdbTimeRange(timestamp,
                             new QdbTimespec(timestamp.asLocalDateTime().plusNanos(1)))
        };

        QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);

        assertThat(results.size(), (is(1)));
        assertThat(results.get(0).getValue(), equalTo(values[0].getDouble()));
    }

    @Test
    public void canInsertMultipleDoubleRows() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));
        QdbTimeSeriesWriter writer = series.tableWriter();

        int ROW_COUNT = 100000;
        QdbTimeSeriesRow[] rows = new QdbTimeSeriesRow[ROW_COUNT];
        for (int i = 0; i < rows.length; ++i) {
            rows[i] =
                new QdbTimeSeriesRow (LocalDateTime.now(),
                                      new QdbTimeSeriesValue[] {
                                          QdbTimeSeriesValue.createDouble(Helpers.randomDouble())});
            writer.append(rows[i]);
        }

        writer.flush();

        QdbTimeRange[] ranges = {
            new QdbTimeRange(rows[0].getTimestamp(),
                             new QdbTimespec(rows[(rows.length - 1)].getTimestamp().asLocalDateTime().plusNanos(1)))
        };

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
        QdbTimeSeriesWriter writer = series.tableWriter();

        QdbTimeSeriesValue[] values = {
            QdbTimeSeriesValue.createSafeBlob(Helpers.createSampleData())
        };

        QdbTimespec timestamp = new QdbTimespec(LocalDateTime.now());
        QdbTimeSeriesRow row = new QdbTimeSeriesRow(timestamp,
                                                    values);
        writer.append(row);
        writer.flush();

        QdbTimeRange[] ranges = {
            new QdbTimeRange(timestamp,
                             new QdbTimespec(timestamp.asLocalDateTime().plusNanos(1)))
        };

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
        QdbTimeSeriesWriter writer = series.tableWriter();

        QdbTimeSeriesValue[] values = {
            QdbTimeSeriesValue.createDouble(Helpers.randomDouble()),
            QdbTimeSeriesValue.createBlob(Helpers.createSampleData())
        };

        QdbTimespec timestamp = new QdbTimespec(LocalDateTime.now());

        writer.append(timestamp, values);
        writer.flush();

        QdbTimeRange[] ranges = {
            new QdbTimeRange(timestamp,
                             new QdbTimespec(timestamp.asLocalDateTime().plusNanos(1)))
        };

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
        QdbTimeSeriesWriter writer = series.tableWriter();

        QdbTimeSeriesValue[] values = {
            QdbTimeSeriesValue.createDouble(Helpers.randomDouble()),
            QdbTimeSeriesValue.createNull()
        };

        QdbTimespec timestamp = new QdbTimespec(LocalDateTime.now());
        QdbTimeSeriesRow row = new QdbTimeSeriesRow(timestamp,
                                                    values);

        writer.append(row);
        writer.flush();

        QdbTimeRange[] ranges = {
            new QdbTimeRange(timestamp,
                             new QdbTimespec(timestamp.asLocalDateTime().plusNanos(1)))
        };

        QdbDoubleColumnCollection results1 = series.getDoubles(alias1, ranges);
        QdbBlobColumnCollection results2 = series.getBlobs(alias2, ranges);

        assertThat(results1.size(), (is(1)));
        assertThat(results2.size(), (is(0)));
        assertThat(results1.get(0).getValue(), equalTo(values[0].getDouble()));
    }

    @Test
    public void writerIsFlushed_whenClosed() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));
        QdbTimeSeriesWriter writer = series.tableWriter();

        QdbTimeSeriesValue[] values = {
            QdbTimeSeriesValue.createDouble(Helpers.randomDouble())
        };

        QdbTimespec timestamp = new QdbTimespec(LocalDateTime.now());
        QdbTimeSeriesRow row = new QdbTimeSeriesRow(timestamp,
                                                    values);
        writer.append(row);
        writer.close();

        QdbTimeRange[] ranges = {
            new QdbTimeRange(timestamp,
                             new QdbTimespec(timestamp.asLocalDateTime().plusNanos(1)))
        };

        QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);

        assertThat(results.size(), (is(1)));
        assertThat(results.get(0).getValue(), equalTo(values[0].getDouble()));
    }

    @Test
    public void autoFlushWriter_isFlushed_whenThresholdReached() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));
        QdbTimeSeriesWriter writer = series.autoFlushTableWriter(2); // flush every 2 rows

        QdbTimeSeriesValue[] values = {
            QdbTimeSeriesValue.createDouble(Helpers.randomDouble())
        };

        QdbTimespec timestamp = new QdbTimespec(LocalDateTime.now());
        QdbTimeSeriesRow row = new QdbTimeSeriesRow(timestamp,
                                                    values);
        writer.append(row);

        QdbTimeRange[] ranges = {
            new QdbTimeRange(timestamp,
                             new QdbTimespec(timestamp.asLocalDateTime().plusNanos(1)))
        };

        QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);

        assertThat(results.size(), (is(0)));

        // Add another row, which should trigger flush
        writer.append(row);

        results = series.getDoubles(alias, ranges);

        assertThat(results.size(), (is(2)));
        assertThat(results.get(0).getValue(), equalTo(values[0].getDouble()));
        assertThat(results.get(1).getValue(), equalTo(values[0].getDouble()));
    }

}
