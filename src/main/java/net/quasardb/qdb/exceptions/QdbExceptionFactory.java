package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

class QdbExceptionFactory {

    public static void throwIfError(error_carrier errorCarrier) {
        throwIfError(new QdbErrorCode(errorCarrier.getError()));
    }

    public static void throwIfError(qdb_error_t errorCode) {
        throwIfError(new QdbErrorCode(errorCode));
    }

    public static void throwIfError(QdbErrorCode errorCode) {
        QdbException exception = createException(errorCode);
        if (exception != null)
            throw exception;
    }

    public static QdbException createException(qdb_error_t errorCode) {
        return createException(new QdbErrorCode(errorCode));
    }

    static QdbException createException(QdbErrorCode errorCode) {
        if (errorCode.severity() == qdb_error_severity_t.error_severity_info)
            return null;

        if (errorCode.origin() == qdb_error_origin_t.error_origin_connection)
            return createConnectionException(errorCode);

        if (errorCode.origin() == qdb_error_origin_t.error_origin_input)
            return createInputException(errorCode);

        if (errorCode.origin() == qdb_error_origin_t.error_origin_operation)
            return createOperationException(errorCode);

        if (errorCode.origin() == qdb_error_origin_t.error_origin_system_local)
            return createLocalSystemException(errorCode);

        if (errorCode.origin() == qdb_error_origin_t.error_origin_system_remote)
            return createRemoteSystemException(errorCode);

        if (errorCode.origin() == qdb_error_origin_t.error_origin_protocol)
            return createProtocolException(errorCode);

        return new QdbException(errorCode.message());
    }

    private static QdbConnectionException createConnectionException(QdbErrorCode errorCode) {
        if (errorCode.equals(qdb_error_t.error_connection_refused))
            return new QdbConnectionRefusedException();

        if (errorCode.equals(qdb_error_t.error_host_not_found))
            return new QdbHostNotFoundException();

        return new QdbConnectionException(errorCode.message());
    }

    private static QdbInputException createInputException(QdbErrorCode errorCode) {
        if (errorCode.equals(qdb_error_t.error_reserved_alias))
            return new QdbReservedAliasException();

        if (errorCode.equals(qdb_error_t.error_invalid_argument))
            return new QdbInvalidArgumentException();

        if (errorCode.equals(qdb_error_t.error_out_of_bounds))
            return new QdbOutOfBoundsException();

        return new QdbInputException(errorCode.message());
    }

    private static QdbLocalSystemException createLocalSystemException(QdbErrorCode errorCode) {
        return new QdbLocalSystemException(errorCode.message());
    }

    private static QdbOperationException createOperationException(QdbErrorCode errorCode) {
        if (errorCode.equals(qdb_error_t.error_alias_not_found))
            return new QdbAliasNotFoundException();

        if (errorCode.equals(qdb_error_t.error_alias_already_exists))
            return new QdbAliasAlreadyExistsException();

        if (errorCode.equals(qdb_error_t.error_incompatible_type))
            return new QdbIncompatibleTypeException();

        if (errorCode.equals(qdb_error_t.error_operation_disabled))
            return new QdbOperationDisabledException();

        if (errorCode.equals(qdb_error_t.error_overflow))
            return new QdbOverflowException();

        if (errorCode.equals(qdb_error_t.error_underflow))
            return new QdbUnderflowException();

        if (errorCode.equals(qdb_error_t.error_resource_locked))
            return new QdbResourceLockedException();

        return new QdbOperationException(errorCode.message());
    }

    private static QdbRemoteSystemException createRemoteSystemException(QdbErrorCode errorCode) {

        return new QdbRemoteSystemException(errorCode.message());
    }

    private static QdbProtocolException createProtocolException(QdbErrorCode errorCode) {
        if (errorCode.equals(qdb_error_t.error_unexpected_reply))
            return new QdbUnexpectedReplyException();

        return new QdbProtocolException(errorCode.message());
    }
}