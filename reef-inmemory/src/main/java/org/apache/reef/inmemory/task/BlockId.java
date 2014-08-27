package org.apache.reef.inmemory.task;

/**
 * An interface for Block ID, to be used to identify blocks cached at each Task.
 * Implementing classes will be used as Map keys. Therefore, they must provide
 * well-formed equals() and hashCode() methods.
 */
public interface BlockId {
  /**
   * Return file path.
   */
  public String getFilePath();

  /**
   * Return offset within file.
   */
  public long getOffset();

  /**
   * Return unique ID. The complementing BlockInfo has the same unique ID.
   */
  public long getUniqueId();

  /**
   * Return size of the block, in MB
   */
  public long getBlockSize();

  /**
   * Compares this BlockId to the specified object. The result is {@code true}
   * if and only if the argument is not {@code null} and is a {@code BlockId}
   * object that represents the same information as this object.
   * @param other The object to compare this {@code BlockId} against
   * @return {@code true} if the given object represents a {@code BlockId}
   * equivalent to this string, {@code false} otherwise
   */
  public boolean equals(Object other);

  /**
   * Returns a hash code for this {@code BlockId}
   * @return  a hash code value for this object.
   */
  public int hashCode();
}
