package net.quasardb.qdb;

/**
 * Exception thrown when the specified alias is reserved for quasardb intenal use.
 */
public class QdbReservedAliasException extends QdbException {

    public QdbReservedAliasException() {
        super("The alias name or prefix is reserved.");
    }
}
