import net.quasardb.qdb.*;
import org.junit.*;

public class QdbBatchRemoveTest {
    String alias;
    QdbInteger entry;
    QdbBatch batch;
    QdbFuture<Void> result;

    @Before
    public void setUp() {
        alias = Helpers.createUniqueAlias();
        entry = Helpers.getInteger(alias);
        batch = Helpers.createBatch();
    }

    @Test(expected = QdbBatchNotRunException.class)
    public void throwsBatchNotRun_beforeCallingRun() {
        result = batch.blob(alias).remove();
        result.get(); // <- throw
    }

    @Test(expected = QdbBatchAlreadyRunException.class)
    public void throwsBatchAlreadyRun_afterCallingRun() {
        batch.run();
        batch.blob(alias).remove(); // <- throw
    }

    @Test(expected = QdbBatchClosedException.class)
    public void throwsBatchClosed_afterCallingClose() {
        batch.close();
        batch.blob(alias).remove(); // <- throw
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_whenAliasIsRandom() {
        result = batch.blob(alias).remove();
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = QdbAliasNotFoundException.class)
    public void throwsAliasNotFound_afterRemove() {
        entry.put(666);
        result = batch.blob(alias).remove();
        entry.remove();
        batch.run();
        result.get(); // <- throws
    }

    @Test(expected = QdbReservedAliasException.class)
    public void throwsReservedAlias_whenAliasIsQdb() {
        alias = Helpers.RESERVED_ALIAS;

        result = batch.blob(alias).remove();
        batch.run();
        result.get(); // <- throws
    }
}
