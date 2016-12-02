package com.algolia.search.http;

import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nonnull;

public abstract class AsyncAlgoliaHttpClient {

  public abstract <T> ListenableFuture<T> requestWithRetry(@Nonnull AlgoliaRequest<T> request);

}
