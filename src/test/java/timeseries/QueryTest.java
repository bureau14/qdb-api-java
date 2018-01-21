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

import net.quasardb.qdb.Helpers;
import net.quasardb.qdb.QdbInputException;
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

    @Test(expected = QdbInputException.class)
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


            System.out.println("result = " + r.toString());
        }

    }
}
