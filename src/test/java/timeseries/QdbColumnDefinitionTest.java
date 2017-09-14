import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class QdbColumnDefinitionTest {
    @Test
    public void canCompareEquality() throws Exception {
        List<QdbColumnDefinition> definitions = Arrays.asList(new QdbColumnDefinition.Blob ("b1"),
                                                              new QdbColumnDefinition.Double ("d1"));

        assertThat(definitions, (hasItems(new QdbColumnDefinition.Blob ("b1"),
                                          new QdbColumnDefinition.Double ("d1"))));
    }
}
