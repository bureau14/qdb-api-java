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
    public void canInsertDoubleRow() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series = Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias)));
        QdbTimeSeriesTable table = series.table();

        QdbTimeSeriesValue[] values = new QdbTimeSeriesValue[1];
        double value = Helpers.randomDouble();
        values[0] = (QdbTimeSeriesValue.createDouble(value));

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
        assertThat(results.get(0).getValue(), equalTo(value));
    }
}
