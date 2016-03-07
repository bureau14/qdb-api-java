import java.nio.ByteBuffer;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;
import org.junit.rules.*;

public class QdbEntryTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void getAlias_returnsSameAlias() {
        String alias = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.getBlob(alias);

        String result = entry.getAlias();

        Assert.assertEquals(alias, result);
    }

    @Test
    public void remove_throwsAliasNotFound_ifCalledTwice() {
        QdbEntry entry = Helpers.createBlob();
        entry.remove();

        exception.expect(QdbAliasNotFoundException.class);
        entry.remove(); // <- throws
    }

    @Test
    public void remove_throwsReservedAlias_whenAliasIsQdb() {
        QdbEntry entry = Helpers.getBlob("qdb");

        exception.expect(QdbReservedAliasException.class);
        entry.remove(); // <- throws
    }

    @Test
    public void addTag_throwsAliasNotFound_whenEntryDoesntExists() {
        String tag = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.createEmptyBlob();

        exception.expect(QdbAliasNotFoundException.class);
        entry.addTag(tag); // <- throws
    }

    @Test
    public void addTag_throwsReserved_whenAliasIsQdb() {
        String tag = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.getBlob("qdb");

        exception.expect(QdbReservedAliasException.class);
        entry.addTag(tag); // <- throws
    }

    @Test
    public void addTag_throwsReserved_whenTagIsQdb() {
        QdbEntry entry = Helpers.createEmptyBlob();

        exception.expect(QdbReservedAliasException.class);
        entry.addTag("qdb"); // <- throws
    }

    @Test
    public void hasTag_throwsAliasNotFound_whenEntryDoesntExists() {
        String tag = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.createEmptyBlob();

        exception.expect(QdbAliasNotFoundException.class);
        entry.hasTag(tag); // <- throws
    }

    @Test
    public void hasTag_throwsReserved_whenAliasIsQdb() {
        String tag = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.getBlob("qdb");

        exception.expect(QdbReservedAliasException.class);
        entry.hasTag(tag); // <- throws
    }

    @Test
    public void hasTag_throwsReserved_whenTagIsQdb() {
        QdbEntry entry = Helpers.createEmptyBlob();

        exception.expect(QdbReservedAliasException.class);
        entry.hasTag("qdb"); // <- throws
    }

    @Test
    public void removeTag_throwsAliasNotFound_whenEntryDoesntExists() {
        String tag = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.createEmptyBlob();

        exception.expect(QdbAliasNotFoundException.class);
        entry.removeTag(tag); // <- throws
    }

    @Test
    public void removeTag_throwsReserved_whenAliasIsQdb() {
        String tag = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.getBlob("qdb");

        exception.expect(QdbReservedAliasException.class);
        entry.removeTag(tag); // <- throws
    }

    @Test
    public void removeTag_throwsReserved_whenTagIsQdb() {
        QdbEntry entry = Helpers.createEmptyBlob();

        exception.expect(QdbReservedAliasException.class);
        entry.removeTag("qdb"); // <- throws
    }

    @Test
    public void getTags_throwsReserved_whenAliasIsQdb() {
        QdbEntry entry = Helpers.getBlob("qdb");

        exception.expect(QdbReservedAliasException.class);
        entry.getTags(); // <- throws
    }
}
