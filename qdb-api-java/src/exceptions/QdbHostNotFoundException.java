package net.quasardb.qdb;

/**
 * Exception thrown when the host name resolution fails.
 */
public class QdbHostNotFoundException extends QdbException {

    public QdbHostNotFoundException() {
        super("The remote host cannot be resolved.");
    }
}
