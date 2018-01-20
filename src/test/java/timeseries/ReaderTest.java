import java.util.*;
import java.time.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;

public class ReaderTest {

    @Test
    public void canGetReader() throws Exception {
        String alias = Helpers.createUniqueAlias();

        TimeRange[] ranges = {
            new TimeRange(Timespec.now(),
                          Timespec.now().plusNanos(1))
        };

        Reader reader =
            Helpers.createTimeSeries(Helpers.generateTableColumns(1)).tableReader(ranges);
    }

    @Test
    public void canCloseReader() throws Exception {
        String alias = Helpers.createUniqueAlias();

        TimeRange[] ranges = {
            new TimeRange(Timespec.now(),
                          Timespec.now().plusNanos(1))
        };

        Reader reader =
            Helpers.createTimeSeries(Helpers.generateTableColumns(1)).tableReader(ranges);
        reader.close();
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void readWithoutRanges_throwsException() throws Exception {
        String alias = Helpers.createUniqueAlias();

        TimeRange[] ranges = {};
        Reader reader =
            Helpers.createTimeSeries(Helpers.generateTableColumns(1)).tableReader(ranges);
    }

    @Test
    public void canReadEmptyResult() throws Exception {
        String alias = Helpers.createUniqueAlias();

        // These ranges should always be empty
        TimeRange[] ranges = {
            new TimeRange(Timespec.now(),
                          Timespec.now().plusNanos(1))
        };

        Reader reader =
            Helpers.createTimeSeries(Helpers.generateTableColumns(1)).tableReader(ranges);

        assertThat(reader.hasNext(), (is(false)));
    }

    @Test
    public void helpersRowGen_generatesDoubleRows() throws Exception {
        Column[] cols = Helpers.generateTableColumns(Value.Type.DOUBLE, 1);
        Row[] rows = Helpers.generateTableRows(cols, 1);

        Arrays.stream(cols)
            .forEach((col) ->
                     assertThat(col.getType(), (equalTo(Value.Type.DOUBLE))));

        Arrays.stream(rows)
            .forEach((row) ->
                     Arrays.stream(row.getValues())
                     .forEach((value) ->
                              assertThat(value.getType(), (equalTo(Value.Type.DOUBLE)))));
    }

    @Test
    public void helpersRowGen_generatesBlobRows() throws Exception {
        Column[] cols = Helpers.generateTableColumns(Value.Type.BLOB, 1);
        Row[] rows = Helpers.generateTableRows(cols, 1);

        Arrays.stream(cols)
            .forEach((col) ->
                     assertThat(col.getType(), (equalTo(Value.Type.BLOB))));

        Arrays.stream(rows)
            .forEach((row) ->
                     Arrays.stream(row.getValues())
                     .forEach((value) ->
                              assertThat(value.getType(), (equalTo(Value.Type.BLOB)))));
    }

    @Test
    public void canReadSingleValue_afterWriting() throws Exception {

        Value.Type[] valueTypes = { Value.Type.INT64,
                                    Value.Type.DOUBLE,
                                    Value.Type.TIMESTAMP,
                                    Value.Type.BLOB };

        for (Value.Type valueType : valueTypes) {
            // Generate a 1x1 test dataset
            Column[] cols =
                Helpers.generateTableColumns(valueType, 1);

            Row[] rows = Helpers.generateTableRows(cols, 1);
            QdbTimeSeries series = Helpers.seedTable(cols, rows);
            TimeRange[] ranges = Helpers.rangesFromRows(rows);

            Reader reader = series.tableReader(ranges);

            assertThat(reader.hasNext(), (is(true)));

            Row row = reader.next();
            assertThat(rows[0], (equalTo(row)));
        }
    }

    @Test
    public void canReadMultipleValues_afterWriting() throws Exception {

        Value.Type[] valueTypes = { Value.Type.INT64,
                                                 Value.Type.DOUBLE,
                                                 Value.Type.TIMESTAMP,
                                                 Value.Type.BLOB };

        for (Value.Type valueType : valueTypes) {
            // Generate a 2x2 test dataset

            Column[] cols =
                Helpers.generateTableColumns(valueType, 2);
            Row[] rows = Helpers.generateTableRows(cols, 2);
            QdbTimeSeries series = Helpers.seedTable(cols, rows);
            TimeRange[] ranges = Helpers.rangesFromRows(rows);

            Reader reader = series.tableReader(ranges);

            int index = 0;
            while (reader.hasNext()) {
                assertThat(rows[index++], (equalTo(reader.next())));
            }
        }
    }

    @Test
    public void canCallHasNext_multipleTimes() throws Exception {
        // Generate a 1x1 test dataset

        Column[] cols = Helpers.generateTableColumns(1);
        Row[] rows = Helpers.generateTableRows(cols, 1);
        QdbTimeSeries series = Helpers.seedTable(cols, rows);
        TimeRange[] ranges = Helpers.rangesFromRows(rows);

        Reader reader = series.tableReader(ranges);

        assertThat(reader.hasNext(), (is(true)));
        assertThat(reader.hasNext(), (is(true)));
        assertThat(reader.hasNext(), (is(true)));

        reader.next();

        assertThat(reader.hasNext(), (is(false)));
        assertThat(reader.hasNext(), (is(false)));
    }

    @Test(expected = QdbInvalidIteratorException.class)
    public void invalidIterator_throwsException() throws Exception {
        // Generate a 1x1 test dataset

        Column[] cols = Helpers.generateTableColumns(1);
        Row[] rows = Helpers.generateTableRows(cols, 1);
        QdbTimeSeries series = Helpers.seedTable(cols, rows);
        TimeRange[] ranges = Helpers.rangesFromRows(rows);

        // Seeding complete, actual test below this line

        Reader reader = series.tableReader(ranges);
        reader.next();
        reader.next();
    }
}
