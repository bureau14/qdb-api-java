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

        QdbTimeRange[] ranges = {};
        QdbTimeSeriesReader reader =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Double (alias))).tableReader(ranges);
    }
}
