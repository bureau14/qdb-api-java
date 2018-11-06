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
        Column[] definition = {
            new Column.Double (alias)
        };
        Writer writer = Helpers.createTimeSeries(definition).tableWriter();
    }

    @Test
    public void canFlushWriter() throws Exception {
        String alias = Helpers.createUniqueAlias();
        Column[] definition = {
            new Column.Double (alias)
        };
        Writer writer = Helpers.createTimeSeries(definition).tableWriter();

        writer.flush();
    }

    @Test
    public void canLookupTableOffsetById() throws Exception {
        Column[] columns = {
            new Column.Double(Helpers.createUniqueAlias()),
            new Column.Double(Helpers.createUniqueAlias()),
            new Column.Double(Helpers.createUniqueAlias()),
            new Column.Double(Helpers.createUniqueAlias()),
            new Column.Double(Helpers.createUniqueAlias()),
            new Column.Double(Helpers.createUniqueAlias()),
            new Column.Double(Helpers.createUniqueAlias()),
            new Column.Double(Helpers.createUniqueAlias())
        };

        Session session = Helpers.getSession();
        Table table1 = Helpers.createTable(columns);
        Table table2 = Helpers.createTable(columns);

        Tables tables = new Tables(new Table[] {table1, table2});

        Writer writer = Tables.writer(session, tables);

        assertThat(writer.tableIndexByName(table1.getName()), equalTo(0));
        assertThat(writer.tableIndexByName(table2.getName()), equalTo(columns.length));
    }

    @Test
    public void canCloseWriter() throws Exception {
        String alias = Helpers.createUniqueAlias();
        Column[] definition = {
            new Column.Double (alias)
        };
        Writer writer = Helpers.createTimeSeries(definition).tableWriter();

        writer.close();
    }

    @Test
    public void canInsertDoubleRow() throws Exception {
        String alias = Helpers.createUniqueAlias();
        Column[] definition = {
            new Column.Double (alias)
        };
        QdbTimeSeries series = Helpers.createTimeSeries(definition);
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
        Column[] definition = {
            new Column.Double (alias)
        };
        QdbTimeSeries series = Helpers.createTimeSeries(definition);
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
        Column[] definition = {
            new Column.Blob (alias)
        };
        QdbTimeSeries series = Helpers.createTimeSeries(definition);
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

        Column[] definition = {
            new Column.Double (alias1),
            new Column.Blob (alias2)
        };

        QdbTimeSeries series = Helpers.createTimeSeries(definition);
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
    public void canInsertMultipleTables() throws Exception {
        String alias1 = Helpers.createUniqueAlias();
        String alias2 = Helpers.createUniqueAlias();
        String alias3 = Helpers.createUniqueAlias();
        String alias4 = Helpers.createUniqueAlias();

        Column[] definition1 = {
            new Column.Double (alias1),
            new Column.Blob (alias2)
        };

        Column[] definition2 = {
            new Column.Double (alias3),
            new Column.Blob (alias4)
        };

        Table table1 = Helpers.createTable(definition1);
        Table table2 = Helpers.createTable(definition2);
        Writer writer = Tables.writer(Helpers.getSession(),
                                      new Tables(new Table[] {table1,
                                                              table2}));

        Value[] values1 = {
            Value.createDouble(Helpers.randomDouble()),
            Value.createBlob(Helpers.createSampleData())
        };

        Value[] values2 = {
            Value.createDouble(Helpers.randomDouble()),
            Value.createBlob(Helpers.createSampleData())
        };

        Timespec timestamp = new Timespec(LocalDateTime.now());

        writer.append(table1.getName(), timestamp, values1);
        writer.append(table2.getName(), timestamp, values2);
        writer.flush();

        TimeRange[] ranges = {
            new TimeRange(timestamp,
                          timestamp.plusNanos(1))
        };


        Reader reader1 = Table.reader(Helpers.getSession(), table1.getName(), ranges);
        Reader reader2 = Table.reader(Helpers.getSession(), table2.getName(), ranges);

        assertThat(reader1.hasNext(), (is(true)));
        assertThat(reader2.hasNext(), (is(true)));

        Row row1 = reader1.next();
        Row row2 = reader2.next();

        assertThat(reader1.hasNext(), (is(false)));
        assertThat(reader2.hasNext(), (is(false)));

        assertThat(row1.getTimestamp(), (equalTo(timestamp)));
        assertThat(row1.getValues(), (equalTo(values1)));

        assertThat(row2.getTimestamp(), (equalTo(timestamp)));
        assertThat(row2.getValues(), (equalTo(values2)));
    }


    @Test
    public void canInsertNullColumns() throws Exception {
        String alias1 = Helpers.createUniqueAlias();
        String alias2 = Helpers.createUniqueAlias();
        Column[] definition = {
            new Column.Double (alias1),
            new Column.Blob (alias2)
        };

        QdbTimeSeries series = Helpers.createTimeSeries(definition);
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
    public void canAddExtraColumnsAfterFlush() throws Exception {
        Session session = Helpers.getSession();

        String alias1 = Helpers.createUniqueAlias();
        String alias2 = Helpers.createUniqueAlias();
        Column[] definition = {
            new Column.Double (alias1),
            new Column.Blob (alias2)
        };

        Table table1 = Helpers.createTable(definition);
        Writer writer = Table.writer(session, table1);

        Timespec timestamp1 = new Timespec(LocalDateTime.now());
        Timespec timestamp2 = timestamp1.plusNanos(1);

        Value[] values1 = {
            Value.createDouble(Helpers.randomDouble()),
            Value.createBlob(Helpers.createSampleData())
        };

        Value[] values2table1 = {
            Value.createDouble(Helpers.randomDouble()),
            Value.createBlob(Helpers.createSampleData())
        };
        Value[] values2table2 = {
            Value.createDouble(Helpers.randomDouble()),
            Value.createBlob(Helpers.createSampleData())
        };
        Value[] values2 = {
            values2table1[0],
            values2table1[1],
            values2table2[0],
            values2table2[1]
        };

        Row row1 = new Row(timestamp1, values1);
        Row row2 = new Row(timestamp2, values2);

        writer.append(row1);
        writer.flush();

        Table table2 = Helpers.createTable(definition);
        writer.extraTables(table2);
        writer.append(row2);
        writer.flush();

        TimeRange[] ranges = {
            new TimeRange(timestamp1, timestamp1.plusNanos(2))
        };

        Reader reader1 = Table.reader(session, table1.getName(), ranges);
        assertThat(reader1.hasNext(), (is(true)));

        Row table1row1 = reader1.next();
        assertThat(table1row1, (is(row1)));

        assertThat(reader1.hasNext(), (is(true)));
        Row table1row2 = reader1.next();
        assertThat(Arrays.equals(table1row2.getValues(), values2table1), (is(true)));
        assertThat(table1row2.getTimestamp(), (is(timestamp2)));
        assertThat(reader1.hasNext(), (is(false)));

        Reader reader2 = Table.reader(session, table2.getName(), ranges);
        assertThat(reader2.hasNext(), (is(true)));
        Row table2row2 = reader2.next();
        assertThat(Arrays.equals(table2row2.getValues(), values2table2), (is(true)));
        assertThat(table2row2.getTimestamp(), (is(timestamp2)));
        assertThat(reader2.hasNext(), (is(false)));
    }

    @Test
    public void canAddExtraColumnsBeforeFlush() throws Exception {
        Session session = Helpers.getSession();

        String alias1 = Helpers.createUniqueAlias();
        String alias2 = Helpers.createUniqueAlias();
        Column[] definition = {
            new Column.Double (alias1),
            new Column.Blob (alias2)
        };

        Table table1 = Helpers.createTable(definition);
        Writer writer = Table.writer(session, table1);

        Timespec timestamp1 = new Timespec(LocalDateTime.now());
        Timespec timestamp2 = timestamp1.plusNanos(1);

        Value[] values1 = {
            Value.createDouble(Helpers.randomDouble()),
            Value.createBlob(Helpers.createSampleData())
        };

        Value[] values2table1 = {
            Value.createDouble(Helpers.randomDouble()),
            Value.createBlob(Helpers.createSampleData())
        };
        Value[] values2table2 = {
            Value.createDouble(Helpers.randomDouble()),
            Value.createBlob(Helpers.createSampleData())
        };
        Value[] values2 = {
            values2table1[0],
            values2table1[1],
            values2table2[0],
            values2table2[1]
        };

        Row row1 = new Row(timestamp1, values1);
        Row row2 = new Row(timestamp2, values2);

        writer.append(row1);

        Table table2 = Helpers.createTable(definition);
        writer.extraTables(table2);
        writer.append(row2);
        writer.flush();

        TimeRange[] ranges = {
            new TimeRange(timestamp1, timestamp1.plusNanos(2))
        };

        Reader reader1 = Table.reader(session, table1.getName(), ranges);
        assertThat(reader1.hasNext(), (is(true)));

        Row table1row1 = reader1.next();
        assertThat(table1row1, (is(row1)));

        assertThat(reader1.hasNext(), (is(true)));
        Row table1row2 = reader1.next();
        assertThat(Arrays.equals(table1row2.getValues(), values2table1), (is(true)));
        assertThat(table1row2.getTimestamp(), (is(timestamp2)));
        assertThat(reader1.hasNext(), (is(false)));

        Reader reader2 = Table.reader(session, table2.getName(), ranges);
        assertThat(reader2.hasNext(), (is(true)));
        Row table2row2 = reader2.next();
        assertThat(Arrays.equals(table2row2.getValues(), values2table2), (is(true)));
        assertThat(table2row2.getTimestamp(), (is(timestamp2)));
        assertThat(reader2.hasNext(), (is(false)));
    }

    @Test
    public void writerIsFlushed_whenClosed() throws Exception {
        String alias = Helpers.createUniqueAlias();
        Column[] definition = {
            new Column.Double (alias)
        };
        QdbTimeSeries series = Helpers.createTimeSeries(definition);
        Writer writer = series.autoFlushTableWriter();

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
        Column[] definition = {
            new Column.Double (alias)
        };
        QdbTimeSeries series = Helpers.createTimeSeries(definition);
        Writer writer = series.autoFlushTableWriter(2);

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
