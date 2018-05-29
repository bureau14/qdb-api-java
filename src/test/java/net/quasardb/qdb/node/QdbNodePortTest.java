import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbNodePortTest {
    @Test
    public void returnsExpectedValue() {
        QdbNode node = Helpers.getNode();

        int port = node.port();

        Assert.assertEquals(Daemon.port(), port);
    }
}
