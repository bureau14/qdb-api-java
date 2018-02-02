package net.quasardb.qdb;

import java.util.*;
import net.quasardb.qdb.exception.ExceptionFactory;
import net.quasardb.qdb.jni.*;

/**
 * An entry that has the ability to expire.
 */
public class QdbExpirableEntry extends QdbEntry {
    protected QdbExpirableEntry(Session session, String alias) {
        super(session, alias);
    }

    /**
     * Sets the expiry time of an existing entry.
     *
     * @param expiryTime The new expiry time of the entry.
     * @throws QdbAliasNotFoundException If the entry does not exist.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbInvalidArgumentException If the expiry time is in the past (with a certain tolerance)
     */
    public void expiryTime(QdbExpiryTime expiryTime) {
        session.throwIfClosed();
        int err = qdb.expires_at(session.handle(), alias, expiryTime.toMillisSinceEpoch());
        ExceptionFactory.throwIfError(err);
    }

    /**
     * Retrieves the expiry time of the entry. A value of zero means the entry never expires.
     *
     * @return The expiry time of the entry.
     * @throws QdbAliasNotFoundException If the entry does not exist.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     */
    public QdbExpiryTime expiryTime() {
        session.throwIfClosed();
        Reference<Long> expiry = new Reference<Long>();
        int err = qdb.get_expiry_time(session.handle(), alias, expiry);
        ExceptionFactory.throwIfError(err);
        return QdbExpiryTime.makeMillisSinceEpoch(expiry.value);
    }
}
