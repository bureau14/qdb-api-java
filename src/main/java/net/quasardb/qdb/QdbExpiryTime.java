package net.quasardb.qdb;

import java.util.*;

public final class QdbExpiryTime {
    private final long secondsSinceEpoch;

    private QdbExpiryTime(long secondsSinceEpoch) {
        this.secondsSinceEpoch = secondsSinceEpoch;
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
        return new QdbExpiryTime(value);
    }

    public static QdbExpiryTime makeMillisSinceEpoch(long value) {
        return makeSecondsSinceEpoch(value / 1000);
    }

    public static QdbExpiryTime makeSecondsFromNow(long delayInSeconds) {
        return makeSecondsSinceEpoch(System.currentTimeMillis() / 1000 + delayInSeconds);
    }

    public static QdbExpiryTime makeMillisFromNow(long delayInMillis) {
        return makeMillisSinceEpoch(System.currentTimeMillis() + delayInMillis);
    }

    public static QdbExpiryTime makeMinutesFromNow(long delayInMinutes) {
        return makeSecondsFromNow(delayInMinutes * 60);
    }

    public long toSecondsSinceEpoch() {
        return secondsSinceEpoch;
    }

    public long toMillisSinceEpoch() {
        return secondsSinceEpoch * 1000;
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
        return obj instanceof QdbExpiryTime && ((QdbExpiryTime)obj).secondsSinceEpoch == secondsSinceEpoch;
    }

    @Override
    public int hashCode() {
        return (new Long(secondsSinceEpoch)).hashCode();
    }

    @Override
    public String toString() {
        return toDate().toString();
    }
}
