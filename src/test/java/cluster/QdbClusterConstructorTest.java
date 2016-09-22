import net.quasardb.qdb.*;
import org.junit.*;

public class QdbClusterConstructorTest {
    @Test(expected = QdbInvalidArgumentException.class)
    public void throwsInvalidArgument_whenUriIsInvalid() {
        new QdbCluster("wrong_uri");
    }

    @Test(expected = QdbConnectionRefusedException.class)
    public void throwsInvalidArgument_whenUriPointsToNothing() {
        new QdbCluster("qdb://127.0.0.1:1");
    }

    @Test(expected = QdbHostNotFoundException.class)
    public void throwsHostNotFound_whenUriContainsANonExistingName() {
        new QdbCluster("qdb://helloworld:666");
    }
}
