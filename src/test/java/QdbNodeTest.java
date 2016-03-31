import net.quasardb.qdb.*;
import org.junit.*;

public class QdbNodeTest {
    @Test
    public void hostName_returnsExpectedValue() {
        QdbNode node = Helpers.getNode();

        String hostName = node.hostName();

        Assert.assertEquals("127.0.0.1", hostName);
    }

    @Test
    public void port_returnsExpectedValue() {
        QdbNode node = Helpers.getNode();

        int port = node.port();

        Assert.assertEquals(DaemonRunner.port(), port);
    }

    private static boolean looksLikeJson(String str) {
        return str.startsWith("{") && str.endsWith("}") && str.length() > 10;
    }

    @Test
    public void config_returnsJson() {
        QdbNode node = Helpers.getNode();

        String config = node.config();

        Assert.assertTrue("This is not JSON: \"" + config + "\"", looksLikeJson(config));
    }

    @Test
    public void status_returnsJson() {
        QdbNode node = Helpers.getNode();

        String status = node.status();

        Assert.assertTrue("This is not JSON: \"" + status + "\"", looksLikeJson(status));
    }

    @Test
    public void topology_returnsJson() {
        QdbNode node = Helpers.getNode();

        String topology = node.topology();

        Assert.assertTrue("This is not JSON: \"" + topology + "\"", looksLikeJson(topology));
    }

    @Test(expected = QdbOperationDisabledException.class)
    public void stop_throwsOperationDisabled() {
        QdbNode node = Helpers.getNode();

        node.stop("hello world"); // <- throws
    }
}