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

    @Test
    public void canAddColumns_afterCreation() throws Exception {
        List<QdbColumn.Definition> definitions1 =
            Arrays.asList(new QdbColumn.Definition.Blob (Helpers.createUniqueAlias()),
                          new QdbColumn.Definition.Double (Helpers.createUniqueAlias()));

        QdbTimeSeries series = Helpers.createTimeSeries(definitions1);

        Iterable<QdbColumn.Definition> result1 = series.listColumns();
        assertThat(result1, (is(definitions1)));

        List<QdbColumn.Definition> definitions2 =
            Arrays.asList(new QdbColumn.Definition.Blob (Helpers.createUniqueAlias()),
                          new QdbColumn.Definition.Double (Helpers.createUniqueAlias()));
        series.insertColumns(definitions2);

        Iterable<QdbColumn.Definition> result2 = series.listColumns();

        List<QdbColumn.Definition> allDefinitions = new ArrayList<QdbColumn.Definition> ();
        allDefinitions.addAll(definitions1);
        allDefinitions.addAll(definitions2);

        assertThat(result2, (is(allDefinitions)));
    }
}
