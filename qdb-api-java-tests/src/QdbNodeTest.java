import net.quasardb.qdb.*;
import org.junit.*;

public class QdbNodeTest {
    @Test
    public void getHostName_returnsExpectedValue() {
        QdbNode node = Helpers.getNode();

        String hostName = node.getHostName();

        Assert.assertEquals("127.0.0.1", hostName);
    }

    @Test
    public void getPort_returnsExpectedValue() {
        QdbNode node = Helpers.getNode();

        int port = node.getPort();

        Assert.assertEquals(2836, port);
    }

    private static boolean looksLikeJson(String str) {
        return str.startsWith("{") && str.endsWith("}") && str.length() > 10;
    }

    @Test
    public void getConfig_returnsJson() {
        QdbNode node = Helpers.getNode();

        String config = node.getConfig();

        Assert.assertTrue("This is not JSON: \"" + config + "\"", looksLikeJson(config));
    }

    @Test
    public void getStatus_returnsJson() {
        QdbNode node = Helpers.getNode();

        String status = node.getStatus();

        Assert.assertTrue("This is not JSON: \"" + status + "\"", looksLikeJson(status));
    }

    @Test
    public void getTopology_returnsJson() {
        QdbNode node = Helpers.getNode();

        String topology = node.getTopology();

        Assert.assertTrue("This is not JSON: \"" + topology + "\"", looksLikeJson(topology));
    }

    @Test(expected = QdbOperationDisabledException.class)
    public void stop_throwsOperationDisabled() {
        QdbNode node = Helpers.getNode();

        node.stop("hello world"); // <- throws
    }
}
