package net.quasardb.qdb;

import java.util.Arrays;
import java.util.Objects;

public final class QdbId {
  private final long data[];

  public QdbId(long a, long b, long c, long d) {
    this.data = new long[] { a, b, c, d };
  }

  public long[] data() {
    return this.data;
  }

  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (!(other instanceof QdbId))
      return false;

    QdbId id = (QdbId) other;
    return Arrays.equals(id.data, this.data);
  }

  public int hashCode() {
    return Objects.hashCode(this.data);
  }

  public String toString() {
    return "[" + Long.toHexString(this.data[0]) + "-" + Long.toHexString(this.data[1]) + "-"
        + Long.toHexString(this.data[2]) + "-" + Long.toHexString(this.data[3]) + "]";
  }
}
