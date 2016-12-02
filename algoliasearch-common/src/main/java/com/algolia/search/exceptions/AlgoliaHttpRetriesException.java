package com.algolia.search.exceptions;

import java.util.Iterator;
import java.util.List;

/**
 * Algolia Exception if all retries failed
 */
public class AlgoliaHttpRetriesException extends AlgoliaException {

  /**
   * List of exception if all retries failed
   */
  private List<AlgoliaIOException> ioExceptionList;

  public AlgoliaHttpRetriesException(String message, List<AlgoliaIOException> ioExceptionList) {
    super(message + ", exceptions: [" + combineMessages(ioExceptionList) + "]");
    this.ioExceptionList = ioExceptionList;
  }

  private static String combineMessages(List<AlgoliaIOException> ioExceptionList) {
    StringBuilder sb = new StringBuilder();
    Iterator<AlgoliaIOException> it = ioExceptionList.iterator();
    while (it.hasNext()) {
      sb.append(it.next().getMessage());
      if (it.hasNext()) sb.append(",");
    }
    return sb.toString();
  }

  @SuppressWarnings("unused")
  public List<AlgoliaIOException> getIoExceptionList() {
    return ioExceptionList;
  }
}
