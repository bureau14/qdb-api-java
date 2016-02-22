package net.quasardb.qdb;

import java.util.*;
import net.quasardb.qdb.jni.*;

/**
 * An entry that has the ability to expire.
 */
public class QdbExpirableEntry extends QdbEntry {

    protected QdbExpirableEntry(SWIGTYPE_p_qdb_session session, String alias) {
        super(session, alias);
    }

    /**
     * Sets the expiry time of an existing entry.
     *
     * @param expiryDate Date at which the the entry expires
     * @throws QdbAliasNotFoundException If the entry does not exist.
     * @throws QdbInvalidArgumentException If the expiration time is in the past (with a certain tolerance)
     */
    public void expiresAt(Date expiryDate) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(expiryDate);
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        expiresAt(cal.getTimeInMillis() / 1000);
    }

    /**
     * Sets the expiry time of an existing entry.
     *
     * @param secondsSinceEpoch The expiration time in seconds since 1970-01-01 00:00:00 UTC
     * @throws QdbAliasNotFoundException If the entry does not exist.
     * @throws QdbInvalidArgumentException If the expiration time is in the past (with a certain tolerance)
     */
    public void expiresAt(long secondsSinceEpoch) {
        qdb_error_t err = qdb.expires_at(session, alias, secondsSinceEpoch);
        QdbExceptionThrower.throwIfError(err);
    }

    /**
     * Sets the expiry time of an existing entry.<br>
     * An expiry time of zero means the entry expires as soon as possible.
     *
     * @param expiryTimeInSeconds time in seconds, relative to the call time, after which the entry expires.
     * @throws QdbException if the entry does not exist or if the expiry time is in the past
     */
    public void expiresFromNow(long expiryTimeInSeconds) {
        qdb_error_t err = qdb.expires_from_now(session, alias, expiryTimeInSeconds);
        QdbExceptionThrower.throwIfError(err);
    }

    /**
     * Retrieves the expiry time of the entry. A value of zero means the entry never expires.
     *
     * @return The absolute expiry time, in seconds since epoch.
     * @throws QdbAliasNotFoundException If the entry does not exist.
     */
    public long getExpiryTime() {
        error_carrier error = new error_carrier();
        long result = qdb.get_expiry(session, alias, error);
        QdbExceptionThrower.throwIfError(error.getError());
        return result;
    }
}