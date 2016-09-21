package net.quasardb.qdb;

import java.util.*;

public final class QdbExpiryTime {
    private final long millisecondsSinceEpoch;

    private QdbExpiryTime(long millisecondsSinceEpoch) {
        this.millisecondsSinceEpoch = millisecondsSinceEpoch;
    }

    public static final QdbExpiryTime NEVER_EXPIRES = new QdbExpiryTime(0);
    public static final QdbExpiryTime PRESERVE_EXPIRATION = new QdbExpiryTime(-1);

    public static QdbExpiryTime fromCalendar(Calendar calendar) {
        return makeMillisSinceEpoch(calendar.getTimeInMillis());
    }

    public static QdbExpiryTime fromDate(Date date) {
        return makeMillisSinceEpoch(date.getTime());
    }

    public static QdbExpiryTime makeSecondsSinceEpoch(long value) {
        return makeMillisSinceEpoch(value * 1000);
    }

    public static QdbExpiryTime makeMillisSinceEpoch(long value) {
        return new QdbExpiryTime(value);
    }

    public static QdbExpiryTime makeSecondsFromNow(long delayInSeconds) {
        return makeMillisFromNow(delayInSeconds * 1000);
    }

    public static QdbExpiryTime makeMillisFromNow(long delayInMillis) {
        return makeMillisSinceEpoch(System.currentTimeMillis() + delayInMillis);
    }

    public static QdbExpiryTime makeMinutesFromNow(long delayInMinutes) {
        return makeSecondsFromNow(delayInMinutes * 60);
    }

    public long toSecondsSinceEpoch() {
        return millisecondsSinceEpoch / 1000;
    }

    public long toMillisSinceEpoch() {
        return millisecondsSinceEpoch;
    }

    public Date toDate() {
        return new Date(toMillisSinceEpoch());
    }

    public Calendar toCalendar() {
        Calendar cal = new GregorianCalendar();
        cal.setTimeInMillis(toMillisSinceEpoch());
        return cal;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof QdbExpiryTime && ((QdbExpiryTime)obj).millisecondsSinceEpoch == millisecondsSinceEpoch;
    }

    @Override
    public int hashCode() {
        return (new Long(millisecondsSinceEpoch)).hashCode();
    }

    @Override
    public String toString() {
        return toDate().toString();
    }
}
