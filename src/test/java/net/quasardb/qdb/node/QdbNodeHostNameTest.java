import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbNodeHostNameTest {
    @Test
    public void returnsExpectedValue() {
        QdbNode node = Helpers.getNode();

        String hostName = node.hostName();

        Assert.assertEquals("127.0.0.1", hostName);
    }
}
