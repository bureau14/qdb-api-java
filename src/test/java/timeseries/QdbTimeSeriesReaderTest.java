import java.util.*;
import java.time.*;
import net.quasardb.qdb.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class QdbTimeSeriesReaderTest {

    @Test
    public void canGetReader() throws Exception {
        String alias = Helpers.createUniqueAlias();

        QdbTimeRange[] ranges = {
            new QdbTimeRange(QdbTimespec.now(),
                             QdbTimespec.now().plusNanos(1))
        };

        QdbTimeSeriesReader reader =
            Helpers.createTimeSeries(Helpers.generateTableColumns(1)).tableReader(ranges);
    }

    @Test
    public void canCloseReader() throws Exception {
        String alias = Helpers.createUniqueAlias();

        QdbTimeRange[] ranges = {
            new QdbTimeRange(QdbTimespec.now(),
                             QdbTimespec.now().plusNanos(1))
        };

        QdbTimeSeriesReader reader =
            Helpers.createTimeSeries(Helpers.generateTableColumns(1)).tableReader(ranges);
        reader.close();
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void readWithoutRanges_throwsException() throws Exception {
        String alias = Helpers.createUniqueAlias();

        QdbTimeRange[] ranges = {};
        QdbTimeSeriesReader reader =
            Helpers.createTimeSeries(Helpers.generateTableColumns(1)).tableReader(ranges);
    }

    @Test
    public void canReadEmptyResult() throws Exception {
        String alias = Helpers.createUniqueAlias();

        // These ranges should always be empty
        QdbTimeRange[] ranges = {
            new QdbTimeRange(QdbTimespec.now(),
                             QdbTimespec.now().plusNanos(1))
        };

        QdbTimeSeriesReader reader =
            Helpers.createTimeSeries(Helpers.generateTableColumns(1)).tableReader(ranges);

        assertThat(reader.hasNext(), (is(false)));
    }

    @Test
    public void canReadSingleDoubleValue_afterWriting() throws Exception {
        // Generate a 1x1 test dataset

        QdbColumnDefinition[] cols = Helpers.generateTableColumns(QdbTimeSeriesValue.Type.DOUBLE, 1);
        QdbTimeSeriesRow[] rows = Helpers.generateTableRows(cols, 1);
        QdbTimeSeries series = Helpers.seedTable(cols, rows);
        QdbTimeRange[] ranges = Helpers.rangesFromRows(rows);

        QdbTimeSeriesReader reader = series.tableReader(ranges);

        assertThat(reader.hasNext(), (is(true)));

        QdbTimeSeriesRow row = reader.next();
        assertThat(rows[0], (equalTo(row)));
    }

    @Test
    public void canReadMultipleValues_afterWriting() throws Exception {
        // Generate a 2x2 test dataset

        QdbColumnDefinition[] cols = Helpers.generateTableColumns(2);
        QdbTimeSeriesRow[] rows = Helpers.generateTableRows(cols, 2);
        QdbTimeSeries series = Helpers.seedTable(cols, rows);
        QdbTimeRange[] ranges = Helpers.rangesFromRows(rows);

        QdbTimeSeriesReader reader = series.tableReader(ranges);

        int index = 0;
        while (reader.hasNext()) {
            assertThat(rows[index++], (equalTo(reader.next())));
        }
    }

    @Test
    public void canCallHasNext_multipleTimes() throws Exception {
        // Generate a 1x1 test dataset

        QdbColumnDefinition[] cols = Helpers.generateTableColumns(1);
        QdbTimeSeriesRow[] rows = Helpers.generateTableRows(cols, 1);
        QdbTimeSeries series = Helpers.seedTable(cols, rows);
        QdbTimeRange[] ranges = Helpers.rangesFromRows(rows);

        QdbTimeSeriesReader reader = series.tableReader(ranges);

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

        QdbColumnDefinition[] cols = Helpers.generateTableColumns(1);
        QdbTimeSeriesRow[] rows = Helpers.generateTableRows(cols, 1);
        QdbTimeSeries series = Helpers.seedTable(cols, rows);
        QdbTimeRange[] ranges = Helpers.rangesFromRows(rows);

        // Seeding complete, actual test below this line

        QdbTimeSeriesReader reader = series.tableReader(ranges);
        reader.next();
        reader.next();
    }
}
