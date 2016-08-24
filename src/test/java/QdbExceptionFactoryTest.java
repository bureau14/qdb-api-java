package net.quasardb.qdb;

import net.quasardb.qdb.jni.qdb_error_t;
import org.junit.*;

public class QdbExceptionFactoryTest {
    @BeforeClass
    public static void loadJniLibrary() {
        QdbJniApi.load();
    }

    @Test
    public void alias_already_exists() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_alias_already_exists);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbOperationException);
        Assert.assertTrue(result instanceof QdbAliasAlreadyExistsException);
    }

    @Test
    public void alias_not_found() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_alias_not_found);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbOperationException);
        Assert.assertTrue(result instanceof QdbAliasNotFoundException);
    }

    @Test
    public void connection_refused() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_connection_refused);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbConnectionException);
        Assert.assertTrue(result instanceof QdbConnectionRefusedException);
    }

    @Test
    public void host_not_found() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_host_not_found);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbConnectionException);
        Assert.assertTrue(result instanceof QdbHostNotFoundException);
    }

    @Test
    public void incompatible_type() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_incompatible_type);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbOperationException);
        Assert.assertTrue(result instanceof QdbIncompatibleTypeException);
    }

    @Test
    public void invalid_protocol() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_invalid_protocol);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbProtocolException);
    }

    @Test
    public void invalid_argument() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_invalid_argument);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbInvalidArgumentException);
    }

    @Test
    public void no_memory_local() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_no_memory_local);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbSystemException);
        Assert.assertTrue(result instanceof QdbLocalSystemException);
    }

    @Test
    public void no_memory_remote() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_no_memory_remote);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbSystemException);
        Assert.assertTrue(result instanceof QdbRemoteSystemException);
    }

    @Test
    public void ok() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_ok);

        Assert.assertNull(result);
    }

    @Test
    public void operation_disabled() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_operation_disabled);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbOperationException);
        Assert.assertTrue(result instanceof QdbOperationDisabledException);
    }

    @Test
    public void out_of_bounds() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_out_of_bounds);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbInputException);
        Assert.assertTrue(result instanceof QdbOutOfBoundsException);
    }

    @Test
    public void overflow() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_overflow);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbOperationException);
        Assert.assertTrue(result instanceof QdbOverflowException);
    }

    @Test
    public void reserved_alias() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_reserved_alias);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbInputException);
        Assert.assertTrue(result instanceof QdbReservedAliasException);
    }

    @Test
    public void underflow() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_underflow);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbOperationException);
        Assert.assertTrue(result instanceof QdbUnderflowException);
    }

    @Test
    public void invalid_reply() {
        Exception result = QdbExceptionFactory.createException(qdb_error_t.error_invalid_reply);

        Assert.assertTrue(result instanceof QdbException);
        Assert.assertTrue(result instanceof QdbProtocolException);
        Assert.assertTrue(result instanceof QdbInvalidReplyException);
    }
}
