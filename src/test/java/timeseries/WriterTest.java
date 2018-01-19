import java.util.*;
import java.time.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;


public class WriterTest {

    @Test
    public void canGetWriter() throws Exception {
        String alias = Helpers.createUniqueAlias();
        Writer writer =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).tableWriter();
    }

    @Test
    public void canFlushWriter() throws Exception {
        String alias = Helpers.createUniqueAlias();
        Writer writer =
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

        Writer writer =
            Helpers.createTimeSeries(Arrays.asList(columns)).tableWriter();

        for (int i = 0; i < columns.length; ++i) {
            QdbColumnDefinition column = columns[i];
            assertThat(writer.getTable().columnIndexById(column.getName()), equalTo(i));
        }
    }


    @Test
    public void canCloseWriter() throws Exception {
        String alias = Helpers.createUniqueAlias();
        Writer writer =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).tableWriter();

        writer.close();
    }

    @Test
    public void canInsertDoubleRow() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));
        Writer writer = series.tableWriter();

        Value[] values = {
            Value.createDouble(Helpers.randomDouble())
        };

        Timespec timestamp = new Timespec(LocalDateTime.now());
        Row row = new Row(timestamp, values);
        writer.append(row);
        writer.flush();

        TimeRange[] ranges = {
            new TimeRange(timestamp,
                          timestamp.plusNanos(1))
        };

        QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);

        assertThat(results.size(), (is(1)));
        assertThat(results.get(0).getValue(), equalTo(values[0].getDouble()));
    }

    @Test
    public void canInsertMultipleDoubleRows() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));
        Writer writer = series.tableWriter();

        int ROW_COUNT = 100000;
        Row[] rows = new Row[ROW_COUNT];
        for (int i = 0; i < rows.length; ++i) {
            rows[i] =
                new Row (LocalDateTime.now(),
                                      new Value[] {
                                          Value.createDouble(Helpers.randomDouble())});
            writer.append(rows[i]);
        }

        writer.flush();

        TimeRange[] ranges = {
            new TimeRange(rows[0].getTimestamp(),
                          new Timespec(rows[(rows.length - 1)].getTimestamp().asLocalDateTime().plusNanos(1)))
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
        Writer writer = series.tableWriter();

        Value[] values = {
            Value.createSafeBlob(Helpers.createSampleData())
        };

        Timespec timestamp = new Timespec(LocalDateTime.now());
        Row row = new Row(timestamp,
                                                    values);
        writer.append(row);
        writer.flush();

        TimeRange[] ranges = {
            new TimeRange(timestamp,
                          timestamp.plusNanos(1))
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
        Writer writer = series.tableWriter();

        Value[] values = {
            Value.createDouble(Helpers.randomDouble()),
            Value.createBlob(Helpers.createSampleData())
        };

        Timespec timestamp = new Timespec(LocalDateTime.now());

        writer.append(timestamp, values);
        writer.flush();

        TimeRange[] ranges = {
            new TimeRange(timestamp,
                          timestamp.plusNanos(1))
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
        Writer writer = series.tableWriter();

        Value[] values = {
            Value.createDouble(Helpers.randomDouble()),
            Value.createNull()
        };

        Timespec timestamp = new Timespec(LocalDateTime.now());
        Row row = new Row(timestamp, values);

        writer.append(row);
        writer.flush();

        TimeRange[] ranges = {
            new TimeRange(timestamp,
                          timestamp.plusNanos(1))
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
        Writer writer = series.tableWriter();

        Value[] values = {
            Value.createDouble(Helpers.randomDouble())
        };

        Timespec timestamp = new Timespec(LocalDateTime.now());
        Row row = new Row(timestamp,
                          values);
        writer.append(row);
        writer.close();

        TimeRange[] ranges = {
            new TimeRange(timestamp,
                          timestamp.plusNanos(1))
        };

        QdbDoubleColumnCollection results = series.getDoubles(alias, ranges);

        assertThat(results.size(), (is(1)));
        assertThat(results.get(0).getValue(), equalTo(values[0].getDouble()));
    }

    @Test
    public void autoFlushWriter_isFlushed_whenThresholdReached() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));
        Writer writer = series.autoFlushTableWriter(2); // flush every 2 rows

        Value[] values = {
            Value.createDouble(Helpers.randomDouble())
        };

        Timespec timestamp = new Timespec(LocalDateTime.now());
        Row row = new Row(timestamp, values);
        writer.append(row);

        TimeRange[] ranges = {
            new TimeRange(timestamp,
                          timestamp.plusNanos(1))
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
