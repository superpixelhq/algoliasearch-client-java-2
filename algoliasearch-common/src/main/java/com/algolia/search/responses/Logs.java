package com.algolia.search.responses;

import com.algolia.search.objects.Log;

import java.util.ArrayList;
import java.util.List;

public class Logs {

  private List<Log> logs;

  public List<Log> getLogs() {
    return logs == null ? new ArrayList<Log>() : logs;
  }

  @SuppressWarnings("unused")
  public Logs setLogs(List<Log> logs) {
    this.logs = logs;
    return this;
  }
}
