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
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).tableReader(ranges);
    }

    @Test
    public void canCloseReader() throws Exception {
        String alias = Helpers.createUniqueAlias();

        QdbTimeRange[] ranges = {
            new QdbTimeRange(QdbTimespec.now(),
                             QdbTimespec.now().plusNanos(1))
        };
        QdbTimeSeriesReader reader =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).tableReader(ranges);
        reader.close();
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void readWithoutRanges_throwsException() throws Exception {
        String alias = Helpers.createUniqueAlias();

        QdbTimeRange[] ranges = {};
        QdbTimeSeriesReader reader =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).tableReader(ranges);
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
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).tableReader(ranges);

        assertThat(reader.hasNext(), (is(false)));
    }

    @Test
    public void canReadSingleValue_afterWriting() throws Exception {
        String alias = Helpers.createUniqueAlias();

        // Prepare single-column timeseries
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));

        // Prepare the single-column row
        QdbTimeSeriesValue[] values = {
            QdbTimeSeriesValue.createDouble(Helpers.randomDouble())
        };

        QdbTimespec timestamp = new QdbTimespec(LocalDateTime.now());
        QdbTimeSeriesRow row = new QdbTimeSeriesRow(timestamp,
                                                    values);

        // These ranges should always be empty
        QdbTimeRange[] ranges = {
            new QdbTimeRange(timestamp,
                             timestamp.plusNanos(1))
        };

        QdbTimeSeriesWriter writer = series.tableWriter();
        writer.append(row);
        writer.flush();

        QdbTimeSeriesReader reader = series.tableReader(ranges);

        assertThat(reader.hasNext(), (is(true)));

    }
}
