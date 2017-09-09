import java.nio.ByteBuffer;
import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class QdbTimeSeriesCreateTest {
    @Test
    public void doesNotThrow_afterCreation() throws Exception {
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumn.Definition.Blob (Helpers.createUniqueAlias()),
                                                   new QdbColumn.Definition.Double (Helpers.createUniqueAlias())));
    }

    @Test
    public void listsColumns_afterCreation() throws Exception {
        List<QdbColumn.Definition> definitions =
            Arrays.asList(new QdbColumn.Definition.Blob (Helpers.createUniqueAlias()),
                          new QdbColumn.Definition.Double (Helpers.createUniqueAlias()));

        QdbTimeSeries series = Helpers.createTimeSeries(definitions);

        Iterable<QdbColumn.Definition> result = series.listColumns();

        assertThat(result, (is(definitions)));
    }
}
