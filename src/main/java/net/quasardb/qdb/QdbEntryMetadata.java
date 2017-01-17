package net.quasardb.qdb;

import java.time.Instant;

public final class QdbEntryMetadata {
  private final QdbId reference;
  private final long size;
  private final Instant lastModificationTime;
  private final Instant expiryTime;

  QdbEntryMetadata(QdbId reference, long size, Instant lastModificationTime, Instant expiryTime) {
    this.reference = reference;
    this.size = size;
    this.lastModificationTime = lastModificationTime;
    this.expiryTime = expiryTime;
  }

  public QdbId reference() {
    return this.reference;
  }

  public long size() {
    return this.size;
  }

  public Instant lastModificationTime() {
    return this.lastModificationTime;
  }

  public Instant expiryTime() {
    return this.expiryTime;
  }
}
