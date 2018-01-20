import java.nio.ByteBuffer;
import java.util.*;
import org.junit.*;
import org.hamcrest.Matcher;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class QdbTimeSeriesCreateTest {
    @Test
    public void doesNotThrow_afterCreation() throws Exception {
        Column[] definitions = {
                new Column.Blob (Helpers.createUniqueAlias()),
                new Column.Double (Helpers.createUniqueAlias())
        };
        QdbTimeSeries series = Helpers.createTimeSeries(definitions);
    }

    @Test
    public void listsColumns_afterCreation() throws Exception {
        Column[] definitions = {
            new Column.Blob (Helpers.createUniqueAlias()),
            new Column.Double (Helpers.createUniqueAlias())
        };

        QdbTimeSeries series = Helpers.createTimeSeries(definitions);

        List<Column> result = Arrays.asList(series.listColumns());

        for (Column definition : definitions) {
            assertThat(result, hasItem(definition));
        }
    }

    @Test
    public void canAddColumns_afterCreation() throws Exception {
        Column[] definitions1 = {
            new Column.Blob (Helpers.createUniqueAlias()),
            new Column.Double (Helpers.createUniqueAlias())
        };

        QdbTimeSeries series = Helpers.createTimeSeries(definitions1);

        List<Column> result1 = Arrays.asList(series.listColumns());
        for (Column definition : definitions1) {
            assertThat(result1, hasItem(definition));
        }

        Column[] definitions2 = {
            new Column.Blob (Helpers.createUniqueAlias()),
            new Column.Double (Helpers.createUniqueAlias())
        };
        series.insertColumns(definitions2);

        List<Column> result2 = Arrays.asList(series.listColumns());

        List<Column> allDefinitions = new ArrayList<Column> ();
        allDefinitions.addAll(Arrays.asList(definitions1));
        allDefinitions.addAll(Arrays.asList(definitions2));

        for (Column definition : allDefinitions) {
            assertThat(result2, hasItem(definition));
        }
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void throws_afterCreatingDuplicateColumns() throws Exception {
        Column[] definitions = {
            new Column.Blob ("b1"),
            new Column.Double ("b1")
        };

        QdbTimeSeries series = Helpers.createTimeSeries(definitions);
    }

    @Test(expected = QdbInvalidArgumentException.class)
    public void throws_afterInsertingDuplicateColumns() throws Exception {
        Column[] definitions1 = {
            new Column.Blob ("b2")
        };

        QdbTimeSeries series = Helpers.createTimeSeries(definitions1);
        Column[] definitions2 = {
            new Column.Double ("b2")
        };
        series.insertColumns(definitions2);
    }
}
