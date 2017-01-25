import net.quasardb.qdb.*;
import org.junit.*;

public class QdbEntryNodeTest {
    @Test
    public void returnsAddressAndPort() {
        QdbBlob blob = Helpers.createEmptyBlob();

        QdbNode node = blob.node();
        Assert.assertEquals("127.0.0.1", node.hostName());
        Assert.assertEquals(DaemonRunner.port(), node.port());
    }
}
