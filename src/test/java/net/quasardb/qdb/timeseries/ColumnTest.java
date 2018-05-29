import java.util.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.*;

public class ColumnTest {
    @Test
    public void canCompareEquality() throws Exception {
        List<Column> definitions = Arrays.asList(new Column.Blob ("b1"),
                                                 new Column.Double ("d1"));

        assertThat(definitions, (hasItems(new Column.Blob ("b1"),
                                          new Column.Double ("d1"))));
    }
}
