import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import org.junit.*;

public class QdbClusterConstructorTest {
    @Test(expected = InvalidArgumentException.class)
    public void throwsInvalidArgument_whenUriIsInvalid() {
        new QdbCluster("wrong_uri");
    }

    @Test(expected = ConnectionRefusedException.class)
    public void throwsInvalidArgument_whenUriPointsToNothing() {
        new QdbCluster("qdb://127.0.0.1:1");
    }

    @Test(expected = HostNotFoundException.class)
    public void throwsHostNotFound_whenUriContainsANonExistingName() {
        new QdbCluster("qdb://helloworld:666");
    }
}
