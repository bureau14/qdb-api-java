import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class QdbColumnTest {
    @Test
    public void creatingDoubleColumn_doesNotThrow() throws Exception {
        QdbDoubleColumn x = new QdbDoubleColumn("d1", 1.23);
    }

    @Test
    public void creatingBlobColumn_doesNotThrow() throws Exception {
        QdbBlobColumn x = new QdbBlobColumn("b1", Helpers.createSampleData());
    }
}
