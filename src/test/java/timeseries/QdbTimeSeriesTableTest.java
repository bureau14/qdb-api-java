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
        QdbTimeSeriesTable table =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).table();

        QdbTimeSeriesValue[] values = new QdbTimeSeriesValue[1];
        values[0] = (QdbTimeSeriesValue.createDouble(Helpers.randomDouble()));

        QdbTimeSeriesRow row = new QdbTimeSeriesRow(new QdbTimespec(LocalDateTime.now()),
                                                    values);

        table.append(row);
        table.append(row);
        table.flush();
    }
}
