import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.StringJoiner;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Query;
import net.quasardb.qdb.ts.QueryBuilder;
import net.quasardb.qdb.ts.Result;
import net.quasardb.qdb.ts.Row;
import net.quasardb.qdb.ts.WritableRow;
import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.TimeRange;

import net.quasardb.qdb.exception.InputException;

import net.quasardb.qdb.Helpers;
import net.quasardb.qdb.QdbTimeSeries;

public class QueryTest {

    @Test
    public void canCreateEmptyQuery() throws Exception {
        Query q = Query.create();
    }

    @Test
    public void canCreateStringyQuery() throws Exception {
        Query q = Query.of("");
    }

    @Test(expected = InputException.class)
    public void cannotExecuteEmptyQuery() throws Exception {
        Query.create()
            .execute(Helpers.getSession());
    }

    @Test
    public void canExecuteValidQuery() throws Exception {
        Value.Type[] valueTypes = { Value.Type.INT64,
                                    Value.Type.DOUBLE,
                                    Value.Type.TIMESTAMP,
                                    Value.Type.BLOB };

        for (Value.Type valueType : valueTypes) {
            Column[] definition =
                Helpers.generateTableColumns(valueType, 1);

            WritableRow[] rows = Helpers.generateTableRows(definition, 1);

            QdbTimeSeries series = Helpers.seedTable(definition, rows);

            Result r = new QueryBuilder()
                .add("select")
                .add(definition[0].getName())
                .add("from")
                .add(series.getName())
                .in(Helpers.rangeFromRows(rows))
                .asQuery()
                .execute(Helpers.getSession());

            assertThat(r.columns.length, (is(definition.length)));
            assertThat(r.rows.length, (is(rows.length)));

            assertThat(r.columns[0], (is(definition[0].getName())));

            Row originalRow = rows[0];
            Row outputRow = r.rows[0];

            assertThat(outputRow, is(originalRow));
        }
    }

    @Test
    public void canAccessResultAsStream() throws Exception {
        Value.Type[] valueTypes = { Value.Type.INT64,
                                    Value.Type.DOUBLE,
                                    Value.Type.TIMESTAMP,
                                    Value.Type.BLOB };

        for (Value.Type valueType : valueTypes) {
            Column[] definition =
                Helpers.generateTableColumns(valueType, 2);

            WritableRow[] rows = Helpers.generateTableRows(definition, 10);

            QdbTimeSeries series = Helpers.seedTable(definition, rows);

            Result r = new QueryBuilder()
                .add("select")
                .add(definition[0].getName())
                .add("from")
                .add(series.getName())
                .in(Helpers.rangeFromRows(rows))
                .asQuery()
                .execute(Helpers.getSession());

            assertThat(r.stream().count(), is(equalTo(Long.valueOf(r.rows.length))));
        }

    }
}
