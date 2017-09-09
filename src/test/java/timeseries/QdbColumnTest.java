import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class QdbColumnTest {
    @Test
    public void canCompareEquality() throws Exception {
        List<QdbColumn.Definition> definitions = Arrays.asList(new QdbColumn.Definition.Blob ("b1"),
                                                                new QdbColumn.Definition.Double ("d1"));

        assertThat(definitions, (hasItems(new QdbColumn.Definition.Blob ("b1"),
                                          new QdbColumn.Definition.Double ("d1"))));
    }
}
