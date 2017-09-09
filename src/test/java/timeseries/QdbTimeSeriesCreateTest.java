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
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Blob (Helpers.createUniqueAlias()),
                                                   new QdbColumnDefinition.Double (Helpers.createUniqueAlias())));
    }

    @Test
    public void listsColumns_afterCreation() throws Exception {
        List<QdbColumnDefinition> definitions =
            Arrays.asList(new QdbColumnDefinition.Blob (Helpers.createUniqueAlias()),
                          new QdbColumnDefinition.Double (Helpers.createUniqueAlias()));

        QdbTimeSeries series = Helpers.createTimeSeries(definitions);

        Iterable<QdbColumnDefinition> result = series.listColumns();

        assertThat(result, (is(definitions)));
    }

    @Test
    public void canAddColumns_afterCreation() throws Exception {
        List<QdbColumnDefinition> definitions1 =
            Arrays.asList(new QdbColumnDefinition.Blob (Helpers.createUniqueAlias()),
                          new QdbColumnDefinition.Double (Helpers.createUniqueAlias()));

        QdbTimeSeries series = Helpers.createTimeSeries(definitions1);

        Iterable<QdbColumnDefinition> result1 = series.listColumns();
        assertThat(result1, (is(definitions1)));

        List<QdbColumnDefinition> definitions2 =
            Arrays.asList(new QdbColumnDefinition.Blob (Helpers.createUniqueAlias()),
                          new QdbColumnDefinition.Double (Helpers.createUniqueAlias()));
        series.insertColumns(definitions2);

        Iterable<QdbColumnDefinition> result2 = series.listColumns();

        List<QdbColumnDefinition> allDefinitions = new ArrayList<QdbColumnDefinition> ();
        allDefinitions.addAll(definitions1);
        allDefinitions.addAll(definitions2);

        assertThat(result2, (is(allDefinitions)));
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void throws_afterCreatingDuplicateColumns() throws Exception {
        List<QdbColumnDefinition> definitions =
            Arrays.asList(new QdbColumnDefinition.Blob ("b1"),
                          new QdbColumnDefinition.Double ("b1"));

        QdbTimeSeries series = Helpers.createTimeSeries(definitions);
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void throws_afterInsertingDuplicateColumns() throws Exception {
        List<QdbColumnDefinition> definitions1 =
            Arrays.asList(new QdbColumnDefinition.Blob ("b2"));

        QdbTimeSeries series = Helpers.createTimeSeries(definitions1);

        List<QdbColumnDefinition> definitions2 =
            Arrays.asList(new QdbColumnDefinition.Double ("b2"));
        series.insertColumns(definitions2);
    }
}
