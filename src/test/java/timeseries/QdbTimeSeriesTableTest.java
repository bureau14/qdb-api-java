import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.*;
import java.util.concurrent.*;
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
}
