import java.nio.ByteBuffer;
import java.util.Date;
import net.quasardb.qdb.*;
import org.junit.*;

public class QdbEntryTest {
    @Test
    public void getAlias_returnsSameAlias() {
        String alias = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.getBlob(alias);

        String result = entry.getAlias();

        Assert.assertEquals(alias, result);
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void remove_throwsAliasNotFound_ifCalledTwice() {
        QdbEntry entry = Helpers.createBlob();

        entry.remove();
        entry.remove(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void remove_throwsReservedAlias_whenAliasIsQdb() {
        QdbEntry entry = Helpers.getBlob("qdb");
        entry.remove(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void addTag_throwsAliasNotFound_whenEntryDoesntExists() {
        String tag = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.createEmptyBlob();

        entry.addTag(tag); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void addTag_throwsReserved_whenAliasIsQdb() {
        String tag = Helpers.createUniqueAlias();

        QdbEntry entry = Helpers.getBlob("qdb");
        entry.addTag(tag); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void addTag_throwsReserved_whenTagIsQdb() {
        QdbEntry entry = Helpers.createEmptyBlob();

        entry.addTag("qdb"); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void hasTag_throwsAliasNotFound_whenEntryDoesntExists() {
        String tag = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.createEmptyBlob();

        entry.hasTag(tag); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void hasTag_throwsReserved_whenAliasIsQdb() {
        String tag = Helpers.createUniqueAlias();

        QdbEntry entry = Helpers.getBlob("qdb");
        entry.hasTag(tag); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void hasTag_throwsReserved_whenTagIsQdb() {
        QdbEntry entry = Helpers.createEmptyBlob();

        entry.hasTag("qdb"); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void removeTag_throwsAliasNotFound_whenEntryDoesntExists() {
        String tag = Helpers.createUniqueAlias();
        QdbEntry entry = Helpers.createEmptyBlob();

        entry.removeTag(tag); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void removeTag_throwsReserved_whenAliasIsQdb() {
        String tag = Helpers.createUniqueAlias();

        QdbEntry entry = Helpers.getBlob("qdb");
        entry.removeTag(tag); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void removeTag_throwsReserved_whenTagIsQdb() {
        QdbEntry entry = Helpers.createEmptyBlob();

        entry.removeTag("qdb"); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void getTags_throwsReserved_whenAliasIsQdb() {
        QdbEntry entry = Helpers.getBlob("qdb");
        entry.getTags(); // <- throws
    }
}
