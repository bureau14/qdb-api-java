package net.quasardb.qdb;

import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.jni.qdb_error;
import org.junit.*;

public class QdbExceptionFactoryTest {
    @Test
    public void alias_already_exists() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.alias_already_exists);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof OperationException);
        Assert.assertTrue(result instanceof AliasAlreadyExistsException);
    }

    @Test
    public void alias_not_found() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.alias_not_found);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof OperationException);
        Assert.assertTrue(result instanceof AliasNotFoundException);
    }

    @Test
    public void connection_refused() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.connection_refused);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof ConnectionException);
        Assert.assertTrue(result instanceof ConnectionRefusedException);
    }

    @Test
    public void host_not_found() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.host_not_found);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof ConnectionException);
        Assert.assertTrue(result instanceof HostNotFoundException);
    }

    @Test
    public void incompatible_type() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.incompatible_type);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof OperationException);
        Assert.assertTrue(result instanceof IncompatibleTypeException);
    }

    @Test
    public void invalid_protocol() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.invalid_protocol);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof ProtocolException);
    }

    @Test
    public void invalid_argument() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.invalid_argument);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof InvalidArgumentException);
    }

    @Test
    public void no_memory_local() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.no_memory_local);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof SystemException);
        Assert.assertTrue(result instanceof LocalSystemException);
    }

    @Test
    public void no_memory_remote() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.no_memory_remote);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof SystemException);
        Assert.assertTrue(result instanceof RemoteSystemException);
    }

    @Test
    public void operation_disabled() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.operation_disabled);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof OperationException);
        Assert.assertTrue(result instanceof OperationDisabledException);
    }

    @Test
    public void out_of_bounds() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.out_of_bounds);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof InputException);
        Assert.assertTrue(result instanceof OutOfBoundsException);
    }

    @Test
    public void overflow() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.overflow);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof OperationException);
        Assert.assertTrue(result instanceof OverflowException);
    }

    @Test
    public void reserved_alias() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.reserved_alias);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof InputException);
        Assert.assertTrue(result instanceof ReservedAliasException);
    }

    @Test
    public void underflow() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.underflow);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof OperationException);
        Assert.assertTrue(result instanceof UnderflowException);
    }

    @Test
    public void invalid_reply() {
        java.lang.Exception result = ExceptionFactory.createException(qdb_error.invalid_reply);

        Assert.assertTrue(result instanceof net.quasardb.qdb.exception.Exception);
        Assert.assertTrue(result instanceof ProtocolException);
        Assert.assertTrue(result instanceof InvalidReplyException);
    }
}
