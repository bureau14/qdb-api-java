package net.quasardb.qdb;

/**
 * Exception thrown when the connection to the cluster is refused.
 */
public class QdbConnectionRefusedException extends QdbException {

    public QdbConnectionRefusedException() {
        super("Connection refused.");
    }
}
