import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;
import org.junit.rules.*;

public class QdbStreamCloseTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void throwsInvalidArgumentOnUnexistingStream_READ() {
        QdbStream stream = Helpers.createEmptyStream();

        exception.expect(QdbInvalidArgumentException.class);
        stream.close();
    }

    @Test
    public void opensAndClosesStream_APPEND() {
        QdbStream stream = Helpers.createEmptyStream();

        stream.open(StandardOpenOption.APPEND);
        stream.close();
    }

    @Test
    public void throwsInvalidArgumentWhenEntryOfDifferentTypeAlreadyExists() {
        String alias = Helpers.createUniqueAlias();
        QdbBlob blob = Helpers.getBlob(alias);
        QdbStream stream = Helpers.getStream(alias);
        ByteBuffer content = Helpers.createSampleData();
        blob.put(content);

        exception.expect(QdbInvalidArgumentException.class);
        stream.close();
    }

}
