package net.quasardb.qdb;

/**
 * Exception thrown when the remote (ie server) operating system caused an error
 */
public class QdbRemoteSystemException extends QdbException {

    public QdbRemoteSystemException(String message) {
        super(message);
    }
}
