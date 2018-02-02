import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.StringJoiner;
import java.util.Arrays;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Query;
import net.quasardb.qdb.ts.QueryBuilder;
import net.quasardb.qdb.ts.Result;
import net.quasardb.qdb.ts.Row;
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

            Row[] rows = Helpers.generateTableRows(definition, 1);
            TimeRange range = Helpers.rangeFromRows(rows);

            QdbTimeSeries series = Helpers.seedTable(definition, rows);

            Result r = new QueryBuilder()
                .add("select")
                .add(definition[0].getName())
                .add("from")
                .add(series.getName())
                .in(range)
                .asQuery()
                .execute(Helpers.getSession());

            Result.Table t = r.tables[0];

            assertThat(r.tables.length, (is(1)));
            assertThat(t.name, (is(series.getName())));


            // Note that there is quite a bit of 'implicit' rules being
            // checked here, specifically that the first column is always
            // the timestamp.
            //
            // These  tests could start failing if the qdb core server's
            // or C api behaviour changes.
            assertThat(t.columns.length, (is(definition.length + 1)));
            assertThat(t.columns[0], (is("timestamp")));
            assertThat(t.columns[1], (is(definition[0].getName())));
            assertThat(t.rows.length, (is(rows.length)));

            int index = 0;
            for (Row originalRow : rows) {
                Value[] row = t.rows[index++];
                assertThat(row.length, (is(definition.length + 1)));

                assertThat(row[0].getType(), (is(Value.Type.TIMESTAMP)));
                assertThat(row[0].getTimestamp(), (is(originalRow.getTimestamp())));

                System.out.println("comparing row value, original = " + originalRow.getValues()[0].toString() + ", from query = " + row[1].toString());
                assertThat(row[1], (is(originalRow.getValues()[0])));
            }
        }

    }
}
