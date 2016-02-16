import java.net.URISyntaxException;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbClusterTest {
    @Test(expected = URISyntaxException.class)
    public void testWrongURI() throws QdbException, URISyntaxException {
        new QdbCluster("wrong_uri");
    }
}
