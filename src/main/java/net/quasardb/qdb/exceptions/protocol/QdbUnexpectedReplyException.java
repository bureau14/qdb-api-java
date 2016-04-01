package net.quasardb.qdb;

/**
 * Exception thrown the response from a remote host cannot be treated.
 */
public final class QdbUnexpectedReplyException extends QdbProtocolException {

    public QdbUnexpectedReplyException() {
        super("Unexpected reply from the remote host.");
    }

    public QdbUnexpectedReplyException(String message) {
        super(message);
    }
}
