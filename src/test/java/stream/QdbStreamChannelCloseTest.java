import java.io.Closeable;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbStreamChannelCloseTest {
    @Test
    public void doesNotThrow_whenCalledTwice() throws Exception {
        QdbStream stream = Helpers.createEmptyStream();

        Closeable channel = stream.open(QdbStream.Mode.APPEND);
        channel.close();
        channel.close();
    }
}
