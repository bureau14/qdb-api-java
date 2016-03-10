package net.quasardb.qdb;

import java.util.*;
import net.quasardb.qdb.jni.*;

/**
 * An entry that has the ability to expire.
 */
public class QdbExpirableEntry extends QdbEntry {
    protected QdbExpirableEntry(QdbSession session, String alias) {
        super(session, alias);
    }

    /**
     * Sets the expiry time of an existing entry.
     *
     * @param expiryTime The new expiry time of the entry.
     * @throws QdbAliasNotFoundException If the entry does not exist.
     * @throws QdbInvalidArgumentException If the expiry time is in the past (with a certain tolerance)
     */
    public void setExpiryTime(QdbExpiryTime expiryTime) {
        qdb_error_t err = qdb.expires_at(session.handle(), alias, expiryTime.toSecondsSinceEpoch());
        QdbExceptionThrower.throwIfError(err);
    }

    /**
     * Retrieves the expiry time of the entry. A value of zero means the entry never expires.
     *
     * @return The absolute expiry time, in seconds since epoch.
     * @throws QdbAliasNotFoundException If the entry does not exist.
     */
    public QdbExpiryTime getExpiryTime() {
        error_carrier error = new error_carrier();
        long secondsSinceEpoch = qdb.get_expiry(session.handle(), alias, error);
        QdbExceptionThrower.throwIfError(error);
        return QdbExpiryTime.makeSecondsSinceEpoch(secondsSinceEpoch);
    }
}