package net.quasardb.qdb;

/**
 * Exception thrown the response from a remote host cannot be treated.
 */
public class QdbUnexpectedReplyException extends QdbException {

    public QdbUnexpectedReplyException() {
        super("Unexpected reply from the remote host.");
    }

    public QdbUnexpectedReplyException(String message) {
        super(message);
    }
}
