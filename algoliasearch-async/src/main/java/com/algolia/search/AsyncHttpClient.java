package com.algolia.search;

import com.algolia.search.http.AlgoliaRequest;
import com.algolia.search.http.AsyncAlgoliaHttpClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;

class AsyncHttpClient extends AsyncAlgoliaHttpClient {

  private final ApacheHttpClient client;
  private final ListeningExecutorService service;

  AsyncHttpClient(AsyncAPIClientConfiguration configuration) {
    this.service = MoreExecutors.listeningDecorator(configuration.getExecutorService());
    this.client = new ApacheHttpClient(configuration);
  }

  @Override
  public <T> ListenableFuture<T> requestWithRetry(@Nonnull final AlgoliaRequest<T> request) {
    return service.submit(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return client.requestWithRetry(request);
      }
    });
  }

}
