package com.algolia.search.inputs;

import com.algolia.search.objects.IndexQuery;
import com.fasterxml.jackson.annotation.JsonInclude;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class MultipleQueriesRequests {

  private final List<QueryWithIndex> requests;

  public MultipleQueriesRequests(List<IndexQuery> requests) {
    this.requests = new ArrayList<>();
    for (IndexQuery indexQuery : requests) {
      this.requests.add(new MultipleQueriesRequests.QueryWithIndex(indexQuery));
    }
  }

  public List<QueryWithIndex> getRequests() {
    return requests;
  }

  private static class QueryWithIndex {

    private final String indexName;
    private final String params;

    QueryWithIndex(@Nonnull IndexQuery q) {
      this.indexName = q.getIndexName();
      this.params = q.getQuery().toParam();
    }

    public String getIndexName() {
      return indexName;
    }

    public String getParams() {
      return params;
    }
  }
}


