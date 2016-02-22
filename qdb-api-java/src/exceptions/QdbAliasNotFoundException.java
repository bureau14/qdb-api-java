package net.quasardb.qdb;

/**
 * Exception thrown when an entry cannot be found in the database
 */
public class QdbAliasNotFoundException extends QdbException {

    public QdbAliasNotFoundException() {
        super("An entry matching the provided alias cannot be found.");
    }
}
