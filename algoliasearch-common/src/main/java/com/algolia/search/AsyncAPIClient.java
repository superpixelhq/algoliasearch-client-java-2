package com.algolia.search;

import com.algolia.search.exceptions.AlgoliaException;
import com.algolia.search.exceptions.AlgoliaIndexNotFoundException;
import com.algolia.search.http.AlgoliaRequest;
import com.algolia.search.http.AsyncAlgoliaHttpClient;
import com.algolia.search.http.HttpMethod;
import com.algolia.search.inputs.*;
import com.algolia.search.inputs.batch.BatchAddObjectOperation;
import com.algolia.search.inputs.batch.BatchDeleteObjectOperation;
import com.algolia.search.inputs.batch.BatchPartialUpdateObjectOperation;
import com.algolia.search.inputs.batch.BatchUpdateObjectOperation;
import com.algolia.search.inputs.partial_update.PartialUpdateOperation;
import com.algolia.search.inputs.synonym.AbstractSynonym;
import com.algolia.search.objects.*;
import com.algolia.search.objects.tasks.async.*;
import com.algolia.search.responses.*;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public class AsyncAPIClient {

  /**
   * Constructor & protected stuff
   */
  protected final AsyncAlgoliaHttpClient httpClient;
  protected final AsyncAPIClientConfiguration configuration;
  protected final Executor executor;

  AsyncAPIClient(AsyncAlgoliaHttpClient httpClient, AsyncAPIClientConfiguration configuration) {
    this.httpClient = httpClient;
    this.configuration = configuration;
    this.executor = configuration.getExecutorService();
  }

  /**
   * All public method
   */

  /**
   * List all existing indexes
   *
   * @return A List of the indices and their metadata
   */
  public ListenableFuture<List<Index.Attributes>> listIndices() {
    ListenableFuture<Indices> result = httpClient.requestWithRetry(
      new AlgoliaRequest<>(HttpMethod.GET, true, Arrays.asList("1", "indexes"), Indices.class)
    );

//    return result.thenApply(Indices::getItems);
    return Futures.transform(result, new Function<Indices, List<Index.Attributes>>() {
      @Nullable @Override public List<Index.Attributes> apply(@Nullable Indices indices) {
        return indices.getItems();
      }
    });
  }

  /**
   * Get the index object initialized (no server call needed for initialization)
   *
   * @param name  name of the index
   * @param klass class of the object in this index
   * @param <T>   the type of the objects in this index
   * @return The initialized index
   */
  public <T> AsyncIndex<T> initIndex(@Nonnull String name, @Nonnull Class<T> klass) {
    return new AsyncIndex<>(name, klass, this);
  }

  /**
   * Get the index object initialized (no server call needed for initialization)
   *
   * @param name name of the index
   * @return The initialized index
   */
  public AsyncIndex<?> initIndex(@Nonnull String name) {
    return new AsyncIndex<>(name, Object.class, this);
  }

  /**
   * Return 10 last log entries.
   *
   * @return A List<Log>
   */
  public ListenableFuture<List<Log>> getLogs() {
    ListenableFuture<Logs> result = httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.GET,
        false,
        Arrays.asList("1", "logs"),
        Logs.class
      )
    );

//    return result.thenApply(Logs::getLogs);
    return Futures.transform(result, new Function<Logs, List<Log>>() {
      @Nullable @Override public List<Log> apply(@Nullable Logs logs) {
        return logs.getLogs();
      }
    });
  }

  /**
   * Return last logs entries
   *
   * @param offset  Specify the first entry to retrieve (0-based, 0 is the most recent log entry)
   * @param length  Specify the maximum number of entries to retrieve starting at offset. Maximum allowed value: 1000
   * @param logType Specify the type of log to retrieve
   * @return The List of Logs
   */
  public ListenableFuture<List<Log>> getLogs(@Nonnull Integer offset, @Nonnull Integer length, @Nonnull LogType logType) {
    Preconditions.checkArgument(offset >= 0, "offset must be >= 0, was %s", offset);
    Preconditions.checkArgument(length >= 0, "length must be >= 0, was %s", length);
    Map<String, String> parameters = new HashMap<>();
    parameters.put("offset", offset.toString());
    parameters.put("length", length.toString());
    parameters.put("type", logType.getName());

    ListenableFuture<Logs> result = httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.GET,
        false,
        Arrays.asList("1", "logs"),
        Logs.class
      ).setParameters(parameters)
    );

//    return result.thenApply(Logs::getLogs);
    return Futures.transform(result, new Function<Logs, List<Log>>() {
      @Nullable @Override public List<Log> apply(@Nullable Logs logs) {
        return logs.getLogs();
      }
    });
  }

  /**
   * List all existing user keys with their associated ACLs
   *
   * @return A List of Keys
   */
  public ListenableFuture<List<ApiKey>> listKeys() {
    ListenableFuture<ApiKeys> result = httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.GET,
        false,
        Arrays.asList("1", "keys"),
        ApiKeys.class
      )
    );

//    return result.thenApply(ApiKeys::getKeys);
    return Futures.transform(result, new Function<ApiKeys, List<ApiKey>>() {
      @Nullable @Override public List<ApiKey> apply(@Nullable ApiKeys apiKeys) {
        return apiKeys.getKeys();
      }
    });
  }

  /**
   * Get an Key from it's name
   *
   * @param key name of the key
   * @return the key
   */
  public ListenableFuture<Optional<ApiKey>> getKey(@Nonnull String key) {
    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.GET,
                    false,
                    Arrays.asList("1", "keys", key),
                    ApiKey.class
            )
    ), new Function<ApiKey, Optional<ApiKey>>() {
      @Nullable @Override public Optional<ApiKey> apply(@Nullable ApiKey apiKey) {
        return Optional.fromNullable(apiKey);
      }
    });
  }

  /**
   * Delete an existing key
   *
   * @param key name of the key
   */
  public ListenableFuture<DeleteKey> deleteKey(@Nonnull String key) {
    return httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.DELETE,
        false,
        Arrays.asList("1", "keys", key),
        DeleteKey.class
      )
    );
  }

  /**
   * Create a new key
   *
   * @param key the key with the ACLs
   * @return the metadata of the key (such as it's name)
   */
  public ListenableFuture<CreateUpdateKey> addKey(@Nonnull ApiKey key) {
    return httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.POST,
        false,
        Arrays.asList("1", "keys"),
        CreateUpdateKey.class
      ).setData(key)
    );
  }

  /**
   * Update a key
   *
   * @param keyName name of the key to update
   * @param key     the key with the ACLs
   * @return the metadata of the key (such as it's name)
   */
  public ListenableFuture<CreateUpdateKey> updateKey(@Nonnull String keyName, @Nonnull ApiKey key) {
    return httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.PUT,
        false,
        Arrays.asList("1", "keys", keyName),
        CreateUpdateKey.class
      ).setData(key)
    );
  }

  /**
   * Generate a secured and public API Key from a query and an
   * optional user token identifying the current user
   *
   * @param privateApiKey your private API Key
   * @param query         contains the parameter applied to the query (used as security)
   */
  @SuppressWarnings("unused")
  public String generateSecuredApiKey(@Nonnull String privateApiKey, @Nonnull Query query) throws AlgoliaException {
    return generateSecuredApiKey(privateApiKey, query, null);
  }

  /**
   * Generate a secured and public API Key from a query and an
   * optional user token identifying the current user
   *
   * @param privateApiKey your private API Key
   * @param query         contains the parameter applied to the query (used as security)
   * @param userToken     an optional token identifying the current user
   */
  @SuppressWarnings("WeakerAccess")
  public String generateSecuredApiKey(@Nonnull String privateApiKey, @Nonnull Query query, String userToken) throws AlgoliaException {
    return Utils.generateSecuredApiKey(privateApiKey, query, userToken);
  }

  /**
   * Wait for the completion of this task
   * /!\ WARNING /!\ This method is blocking
   *
   * @param task       the task to wait
   * @param timeToWait the time to wait in milliseconds
   */
  public <T> void waitTask(@Nonnull AsyncGenericTask<T> task, long timeToWait) {
    Preconditions.checkArgument(timeToWait >= 0, "timeToWait must be >= 0, was %s", timeToWait);
    while (true) {
      ListenableFuture<TaskStatus> status = httpClient.requestWithRetry(
        new AlgoliaRequest<>(
          HttpMethod.GET,
          false,
          Arrays.asList("1", "indexes", task.getIndexName(), "task", task.getTaskIDToWaitFor().toString()),
          TaskStatus.class
        )
      );

      TaskStatus result;
      try {
        result = status.get();
      } catch (CancellationException | InterruptedException | ExecutionException e) {
        //If the future was cancelled or the thread was interrupted or future completed exceptionally
        //We stop
        break;
      }

      if (Objects.equals("published", result.getStatus())) {
        return;
      }
      try {
        Thread.sleep(timeToWait);
      } catch (InterruptedException ignored) {
      }
      timeToWait *= 2;
      timeToWait = timeToWait > Defaults.MAX_TIME_MS_TO_WAIT ? Defaults.MAX_TIME_MS_TO_WAIT : timeToWait;
    }
  }

  /**
   * Custom batch
   * <p>
   * All operations must have a valid index name (not null)
   *
   * @param operations the list of operations to perform
   * @return the associated task
   */
  public ListenableFuture<AsyncTasksMultipleIndex> batch(@Nonnull List<BatchOperation> operations) {
//    boolean atLeastOneHaveIndexNameNull = operations.stream().anyMatch(o -> o.getIndexName() == null);
//    if (atLeastOneHaveIndexNameNull) {
//      return Utils.completeExceptionally(new AlgoliaException("All batch operations must have an index name set"));
//    }
    for (BatchOperation operation : operations) {
      if (operation.getIndexName() == null) return Utils.completeExceptionally(new AlgoliaException("All batch operations must have an index name set"));
    }

//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.POST,
//        true,
//        Arrays.asList("1", "indexes", "*", "batch"),
//        AsyncTasksMultipleIndex.class
//      ).setData(new BatchOperations(operations))
//    ).thenApply(AsyncTasksMultipleIndex::computeIndex);
    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.POST,
                    true,
                    Arrays.asList("1", "indexes", "*", "batch"),
                    AsyncTasksMultipleIndex.class
            ).setData(new BatchOperations(operations))
    ), new Function<AsyncTasksMultipleIndex, AsyncTasksMultipleIndex>() {
      @Override public AsyncTasksMultipleIndex apply(AsyncTasksMultipleIndex index) {
        return index.computeIndex();
      }
    });
  }

  /**
   * Performs multiple searches on multiple indices with the strategy <code>MultiQueriesStrategy.NONE</code>
   *
   * @param queries the queries
   * @return the result of the queries
   */
  @SuppressWarnings("unused")
  public ListenableFuture<MultiQueriesResult> multipleQueries(@Nonnull List<IndexQuery> queries) {
    return multipleQueries(queries, MultiQueriesStrategy.NONE);
  }

  /**
   * Performs multiple searches on multiple indices
   *
   * @param queries  the queries
   * @param strategy the strategy to apply to this multiple queries
   * @return the result of the queries
   */
  @SuppressWarnings("WeakerAccess")
  public ListenableFuture<MultiQueriesResult> multipleQueries(@Nonnull List<IndexQuery> queries, @Nonnull MultiQueriesStrategy strategy) {
    return httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.POST,
        true,
        Arrays.asList("1", "indexes", "*", "queries"),
        MultiQueriesResult.class
      )
        .setData(new MultipleQueriesRequests(queries))
        .setParameters(ImmutableMap.of("strategy", strategy.getName()))
    );
  }

  /**
   * Package protected method for the Index class
   **/

  ListenableFuture<AsyncTask> moveIndex(final String srcIndexName, String dstIndexName) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.POST,
//        false,
//        Arrays.asList("1", "indexes", srcIndexName, "operation"),
//        AsyncTask.class
//      ).setData(new OperationOnIndex("move", dstIndexName))
//    ).thenApply(s -> s.setIndex(srcIndexName));
    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.POST,
                    false,
                    Arrays.asList("1", "indexes", srcIndexName, "operation"),
                    AsyncTask.class
            ).setData(new OperationOnIndex("move", dstIndexName))
    ), new Function<AsyncTask, AsyncTask>() {
      @Override
      public AsyncTask apply(AsyncTask task) {
        return task.setIndex(srcIndexName);
      }
    });
  }

  ListenableFuture<AsyncTask> copyIndex(final String srcIndexName, String dstIndexName) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.POST,
//        false,
//        Arrays.asList("1", "indexes", srcIndexName, "operation"),
//        AsyncTask.class
//      ).setData(new OperationOnIndex("copy", dstIndexName))
//    ).thenApply(s -> s.setIndex(srcIndexName));
    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.POST,
                    false,
                    Arrays.asList("1", "indexes", srcIndexName, "operation"),
                    AsyncTask.class
            ).setData(new OperationOnIndex("copy", dstIndexName))
    ), new Function<AsyncTask, AsyncTask>() {
      @Override
      public AsyncTask apply(AsyncTask task) {
        return task.setIndex(srcIndexName);
      }
    });
  }

  ListenableFuture<AsyncTask> deleteIndex(final String indexName) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.DELETE,
//        false,
//        Arrays.asList("1", "indexes", indexName),
//        AsyncTask.class
//      )
//    ).thenApply(s -> s.setIndex(indexName));


    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.DELETE,
                    false,
                    Arrays.asList("1", "indexes", indexName),
                    AsyncTask.class
            )
    ), new Function<AsyncTask, AsyncTask>() {
      @Override
      public AsyncTask apply(AsyncTask task) {
        return task.setIndex(indexName);
      }
    });
  }

  <T> ListenableFuture<AsyncTaskIndexing> addObject(final String indexName, T object) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.POST,
//        false,
//        Arrays.asList("1", "indexes", indexName),
//        AsyncTaskIndexing.class
//      ).setData(object)
//    ).thenApply(s -> s.setIndex(indexName));

    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.POST,
                    false,
                    Arrays.asList("1", "indexes", indexName),
                    AsyncTaskIndexing.class
            ).setData(object)
    ), new Function<AsyncTaskIndexing, AsyncTaskIndexing>() {
      @Override
      public AsyncTaskIndexing apply(AsyncTaskIndexing task) {
        return task.setIndex(indexName);
      }
    });
  }

  <T> ListenableFuture<AsyncTaskIndexing> addObject(final String indexName, String objectID, T object) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.PUT,
//        false,
//        Arrays.asList("1", "indexes", indexName, objectID),
//        AsyncTaskIndexing.class
//      ).setData(object)
//    ).thenApply(s -> s.setIndex(indexName));

    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.PUT,
                    false,
                    Arrays.asList("1", "indexes", indexName, objectID),
                    AsyncTaskIndexing.class
            ).setData(object)
    ), new Function<AsyncTaskIndexing, AsyncTaskIndexing>() {
      @Override
      public AsyncTaskIndexing apply(AsyncTaskIndexing task) {
        return task.setIndex(indexName);
      }
    });
  }

  <T> ListenableFuture<Optional<T>> getObject(String indexName, String objectID, Class<T> klass) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.GET,
//        true,
//        Arrays.asList("1", "indexes", indexName, objectID),
//        klass
//      )
//    ).thenApply(Optional::ofNullable);

    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.GET,
                    true,
                    Arrays.asList("1", "indexes", indexName, objectID),
                    klass
            )
    ), new Function<T, Optional<T>>() {
      @Override public Optional<T> apply(@Nullable T obj) {
        return Optional.fromNullable(obj);
      }
    });
  }

  <T> ListenableFuture<AsyncTaskSingleIndex> addObjects(final String indexName, List<T> objects) {
    return Futures.transform(batchSingleIndex(
      indexName,
//      objects.stream().map(BatchAddObjectOperation::new).collect(Collectors.toList())
      Lists.newArrayList(Iterables.transform(objects, new Function<T, BatchOperation>() {
        @Override public BatchOperation apply(T obj) {
          return new BatchAddObjectOperation<>(obj);
        }
      }))
    ), new Function<AsyncTaskSingleIndex, AsyncTaskSingleIndex>() {
      @Override
      public AsyncTaskSingleIndex apply(AsyncTaskSingleIndex task) {
        return task.setIndex(indexName);
      }
    });
  }

  private ListenableFuture<AsyncTaskSingleIndex> batchSingleIndex(final String indexName, List<BatchOperation> operations) {
    return Futures.transform(httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.POST,
        false,
        Arrays.asList("1", "indexes", indexName, "batch"),
        AsyncTaskSingleIndex.class
      ).setData(new Batch(operations))
    ), new Function<AsyncTaskSingleIndex, AsyncTaskSingleIndex>() {
      @Override
      public AsyncTaskSingleIndex apply(AsyncTaskSingleIndex task) {
        return task.setIndex(indexName);
      }
    });
  }

  <T> ListenableFuture<AsyncTask> saveObject(final String indexName, String objectID, T object) {
    return Futures.transform(httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.PUT,
        false,
        Arrays.asList("1", "indexes", indexName, objectID),
        AsyncTask.class
      ).setData(object)
    ), new Function<AsyncTask, AsyncTask>() {
      @Override
      public AsyncTask apply(AsyncTask task) {
        return task.setIndex(indexName);
      }
    });
  }

  <T> ListenableFuture<AsyncTaskSingleIndex> saveObjects(final String indexName, List<T> objects) {
    return Futures.transform(batchSingleIndex(
      indexName,
//      objects.stream().map(BatchUpdateObjectOperation::new).collect(Collectors.toList())
      Lists.newArrayList(Iterables.transform(objects, new Function<T, BatchOperation>() {
        @Override public BatchOperation apply(T obj) {
          return new BatchUpdateObjectOperation<>(obj);
        }
      }))
    ), new Function<AsyncTaskSingleIndex, AsyncTaskSingleIndex>() {
      @Override
      public AsyncTaskSingleIndex apply(AsyncTaskSingleIndex task) {
        return task.setIndex(indexName);
      }
    });
  }

  ListenableFuture<AsyncTask> deleteObject(final String indexName, String objectID) {
    return Futures.transform(httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.DELETE,
        false,
        Arrays.asList("1", "indexes", indexName, objectID),
        AsyncTask.class
      )
    ), new Function<AsyncTask, AsyncTask>() {
      @Override
      public AsyncTask apply(AsyncTask task) {
        return task.setIndex(indexName);
      }
    });
  }

  ListenableFuture<AsyncTaskSingleIndex> deleteObjects(final String indexName, List<String> objectIDs) {
    return Futures.transform(batchSingleIndex(
      indexName,
//      objectIDs.stream().map(BatchDeleteObjectOperation::new).collect(Collectors.toList())
      Lists.newArrayList(Iterables.transform(objectIDs, new Function<String, BatchOperation>() {
        @Override public BatchOperation apply(String objectID) {
          return new BatchDeleteObjectOperation(objectID);
        }
      }))
    ), new Function<AsyncTaskSingleIndex, AsyncTaskSingleIndex>() {
      @Override
      public AsyncTaskSingleIndex apply(AsyncTaskSingleIndex task) {
        return task.setIndex(indexName);
      }
    });
  }

  ListenableFuture<AsyncTask> clearIndex(final String indexName) {
    return Futures.transform(httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.POST,
        false,
        Arrays.asList("1", "indexes", indexName, "clear"),
        AsyncTask.class
      )
    ), new Function<AsyncTask, AsyncTask>() {
      @Override
      public AsyncTask apply(AsyncTask task) {
        return task.setIndex(indexName);
      }
    });
  }

  @SuppressWarnings("unchecked")
  <T> ListenableFuture<List<T>> getObjects(final String indexName, List<String> objectIDs, Class<T> klass) {
//    Requests requests = new Requests(objectIDs.stream().map(o -> new Requests.Request().setIndexName(indexName).setObjectID(o)).collect(Collectors.toList()));
    Requests requests = new Requests(Lists.newArrayList(Iterables.transform(objectIDs, new Function<String, Requests.Request>(){
      @Nullable @Override public Requests.Request apply(@Nullable String objectID) {
        return new Requests.Request().setIndexName(indexName).setObjectID(objectID);
      }
    })));

    AlgoliaRequest<Results> algoliaRequest = new AlgoliaRequest<>(
      HttpMethod.POST,
      true,
      Arrays.asList("1", "indexes", "*", "objects"),
      Results.class,
      klass
    );

    return Futures.transform(httpClient
                  .requestWithRetry(algoliaRequest.setData(requests)),
//      .thenApply(Results::getResults);
            new Function<Results, List<T>>() {
              @Nullable
              @Override
              public List<T> apply(Results results) {
                return results.getResults();
              }
            });
  }

  @SuppressWarnings("unchecked")
  <T> ListenableFuture<List<T>> getObjects(final String indexName, List<String> objectIDs, List<String> attributesToRetrieve, Class<T> klass) {
    final String encodedAttributesToRetrieve = Joiner.on(",").join(attributesToRetrieve);
//    Requests requests = new Requests(objectIDs.stream().map(o -> new Requests.Request().setIndexName(indexName).setObjectID(o).setAttributesToRetrieve(encodedAttributesToRetrieve)).collect(Collectors.toList()));
    Requests requests = new Requests(Lists.newArrayList(Iterables.transform(objectIDs, new Function<String, Requests.Request>(){
      @Nullable @Override public Requests.Request apply(@Nullable String objectID) {
        return new Requests.Request().setIndexName(indexName).setObjectID(objectID).setAttributesToRetrieve(encodedAttributesToRetrieve);
      }
    })));

    AlgoliaRequest<Results> algoliaRequest = new AlgoliaRequest<>(
      HttpMethod.POST,
      true,
      Arrays.asList("1", "indexes", "*", "objects"),
      Results.class,
      klass
    );

    return Futures.transform(httpClient
                  .requestWithRetry(algoliaRequest.setData(requests)),
//      .thenApply(Results::getResults);
            new Function<Results, List<T>>() {
              @Nullable
              @Override
              public List<T> apply(Results results) {
                return results.getResults();
              }
            });
  }

  ListenableFuture<IndexSettings> getSettings(String indexName) {
    return httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.GET,
        true,
        Arrays.asList("1", "indexes", indexName, "settings"),
        IndexSettings.class
      ).setParameters(ImmutableMap.of("getVersion", "2"))
    );
  }

  ListenableFuture<AsyncTask> setSettings(final String indexName, IndexSettings settings, Boolean forwardToReplicas) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.PUT,
//        false,
//        Arrays.asList("1", "indexes", indexName, "settings"),
//        AsyncTask.class
//      )
//        .setData(settings)
//        .setParameters(ImmutableMap.of("forwardToReplicas", forwardToReplicas.toString()))
//    ).thenApply(s -> s.setIndex(indexName));

    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.PUT,
                    false,
                    Arrays.asList("1", "indexes", indexName, "settings"),
                    AsyncTask.class
            )
                    .setData(settings)
                    .setParameters(ImmutableMap.of("forwardToReplicas", forwardToReplicas.toString()))
    ), new Function<AsyncTask, AsyncTask>() {
      @Override
      public AsyncTask apply(AsyncTask task) {
        return task.setIndex(indexName);
      }
    });
  }

  ListenableFuture<List<ApiKey>> listKeys(String indexName) {
    ListenableFuture<ApiKeys> result = httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.GET,
        false,
        Arrays.asList("1", "indexes", indexName, "keys"),
        ApiKeys.class
      )
    );

//    return result.thenApply(ApiKeys::getKeys);
    return Futures.transform(result, new Function<ApiKeys, List<ApiKey>>() {
      @Nullable
      @Override
      public List<ApiKey> apply(ApiKeys apiKeys) {
        return apiKeys.getKeys();
      }
    });
  }

  ListenableFuture<Optional<ApiKey>> getKey(String indexName, String key) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.GET,
//        false,
//        Arrays.asList("1", "indexes", indexName, "keys", key),
//        ApiKey.class
//      )
//    ).thenApply(Optional::ofNullable);

    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.GET,
                    false,
                    Arrays.asList("1", "indexes", indexName, "keys", key),
                    ApiKey.class
            )
    ), new Function<ApiKey, Optional<ApiKey>>() {
      @Override
      public Optional<ApiKey> apply(@Nullable ApiKey apiKey) {
        return Optional.fromNullable(apiKey);
      }
    });
  }

  ListenableFuture<DeleteKey> deleteKey(String indexName, String key) {
    return httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.DELETE,
        false,
        Arrays.asList("1", "indexes", indexName, "keys", key),
        DeleteKey.class
      )
    );
  }

  ListenableFuture<CreateUpdateKey> addKey(String indexName, ApiKey key) {
    return httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.POST,
        false,
        Arrays.asList("1", "indexes", indexName, "keys"),
        CreateUpdateKey.class
      ).setData(key)
    );
  }

  ListenableFuture<CreateUpdateKey> updateKey(String indexName, String keyName, ApiKey key) {
    return httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.PUT,
        false,
        Arrays.asList("1", "indexes", indexName, "keys", keyName),
        CreateUpdateKey.class
      ).setData(key)
    );
  }

  @SuppressWarnings("unchecked")
  <T> ListenableFuture<SearchResult<T>> search(final String indexName, Query query, Class<T> klass) {
    AlgoliaRequest<SearchResult<T>> algoliaRequest = new AlgoliaRequest(
      HttpMethod.POST,
      true,
      Arrays.asList("1", "indexes", indexName, "query"),
      SearchResult.class,
      klass
    );

//    return httpClient
//      .requestWithRetry(algoliaRequest.setData(new Search(query)))
//      .thenCompose(result -> {
//        ListenableFuture<SearchResult<T>> r = new ListenableFuture<>();
//        if(result == null) { //Special case when the index does not exists
//          r.completeExceptionally(new AlgoliaIndexNotFoundException(indexName + " does not exist"));
//        } else {
//          r.complete(result);
//        }
//        return r;
//      });

    return Futures.transformAsync(
            httpClient.requestWithRetry(algoliaRequest.setData(new Search(query))),
            new AsyncFunction<SearchResult<T>, SearchResult<T>>() {
              @Override
              public ListenableFuture<SearchResult<T>> apply(final @Nullable SearchResult<T> result) throws Exception {
                if(result == null) { //Special case when the index does not exists
                  return Utils.completeExceptionally(new AlgoliaIndexNotFoundException(indexName + " does not exist"));
                } else {
                  return ListenableFutureTask.create(new Callable<SearchResult<T>>() {
                    @Override
                    public SearchResult<T> call() throws Exception {
                      return result;
                    }
                  });
                }
              }
            }
    );
  }

  ListenableFuture<AsyncTaskSingleIndex> batch(final String indexName, List<BatchOperation> operations) {
    //Special case for single index batches, indexName of operations should be null
//    boolean onSameIndex = operations.stream().allMatch(o -> Objects.equals(null, o.getIndexName()));
//    if (!onSameIndex) {
//      Utils.completeExceptionally(new AlgoliaException("All operations are not on the same index"));
//    }
    for (BatchOperation operation : operations) {
      if (!Objects.equals(null, operation.getIndexName())) {
        Utils.completeExceptionally(new AlgoliaException("All operations are not on the same index"));
      }
    }

//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.POST,
//        false,
//        Arrays.asList("1", "indexes", indexName, "batch"),
//        AsyncTaskSingleIndex.class
//      ).setData(new BatchOperations(operations))
//    ).thenApply(s -> s.setIndex(indexName));


    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.POST,
                    false,
                    Arrays.asList("1", "indexes", indexName, "batch"),
                    AsyncTaskSingleIndex.class
            ).setData(new BatchOperations(operations))
    ), new Function<AsyncTaskSingleIndex, AsyncTaskSingleIndex>() {
      @Override
      public AsyncTaskSingleIndex apply(AsyncTaskSingleIndex task) {
        return task.setIndex(indexName);
      }
    });
  }

  ListenableFuture<AsyncTaskSingleIndex> partialUpdateObject(final String indexName, PartialUpdateOperation operation, Boolean createIfNotExists) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.POST,
//        false,
//        Arrays.asList("1", "indexes", indexName, operation.getObjectID(), "partial"),
//        AsyncTaskSingleIndex.class
//      ).setParameters(ImmutableMap.of("createIfNotExists", createIfNotExists.toString())).setData(operation.toSerialize())
//    ).thenApply(s -> s.setIndex(indexName));


    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.POST,
                    false,
                    Arrays.asList("1", "indexes", indexName, operation.getObjectID(), "partial"),
                    AsyncTaskSingleIndex.class
            ).setParameters(ImmutableMap.of("createIfNotExists", createIfNotExists.toString())).setData(operation.toSerialize())
    ), new Function<AsyncTaskSingleIndex, AsyncTaskSingleIndex>() {
      @Override
      public AsyncTaskSingleIndex apply(AsyncTaskSingleIndex task) {
        return task.setIndex(indexName);
      }
    });
  }

  ListenableFuture<AsyncTaskSingleIndex> partialUpdateObject(final String indexName, String objectID, Object object) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.POST,
//        false,
//        Arrays.asList("1", "indexes", indexName, objectID, "partial"),
//        AsyncTaskSingleIndex.class
//      ).setData(object)
//    ).thenApply(s -> s.setIndex(indexName));


    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.POST,
                    false,
                    Arrays.asList("1", "indexes", indexName, objectID, "partial"),
                    AsyncTaskSingleIndex.class
            ).setData(object)
    ), new Function<AsyncTaskSingleIndex, AsyncTaskSingleIndex>() {
      @Override
      public AsyncTaskSingleIndex apply(AsyncTaskSingleIndex task) {
        return task.setIndex(indexName);
      }
    });
  }

  ListenableFuture<AsyncTask> saveSynonym(final String indexName, String synonymID, AbstractSynonym content, Boolean forwardToReplicas, Boolean replaceExistingSynonyms) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.PUT,
//        false,
//        Arrays.asList("1", "indexes", indexName, "synonyms", synonymID),
//        AsyncTask.class
//      ).setParameters(ImmutableMap.of("forwardToReplicas", forwardToReplicas.toString(), "replaceExistingSynonyms", replaceExistingSynonyms.toString())).setData(content)
//    ).thenApply(s -> s.setIndex(indexName));


    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.PUT,
                    false,
                    Arrays.asList("1", "indexes", indexName, "synonyms", synonymID),
                    AsyncTask.class
            ).setParameters(ImmutableMap.of("forwardToReplicas", forwardToReplicas.toString(), "replaceExistingSynonyms", replaceExistingSynonyms.toString())).setData(content)
    ), new Function<AsyncTask, AsyncTask>() {
      @Override
      public AsyncTask apply(AsyncTask task) {
        return task.setIndex(indexName);
      }
    });
  }

  ListenableFuture<Optional<AbstractSynonym>> getSynonym(String indexName, String synonymID) {
    return Futures.transform(httpClient
            .requestWithRetry(
                    new AlgoliaRequest<>(
                            HttpMethod.GET,
                            false,
                            Arrays.asList("1", "indexes", indexName, "synonyms", synonymID),
                            AbstractSynonym.class
                    )
            ), new Function<AbstractSynonym, Optional<AbstractSynonym>>() {
      @Override
      public Optional<AbstractSynonym> apply(@Nullable AbstractSynonym abstractSynonym) {
        return Optional.fromNullable(abstractSynonym);
      }
    });
  }

  ListenableFuture<AsyncTask> deleteSynonym(final String indexName, String synonymID, Boolean forwardToReplicas) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.DELETE,
//        false,
//        Arrays.asList("1", "indexes", indexName, "synonyms", synonymID),
//        AsyncTask.class
//      ).setParameters(ImmutableMap.of("forwardToReplicas", forwardToReplicas.toString()))
//    ).thenApply(s -> s.setIndex(indexName));

    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.DELETE,
                    false,
                    Arrays.asList("1", "indexes", indexName, "synonyms", synonymID),
                    AsyncTask.class
            ).setParameters(ImmutableMap.of("forwardToReplicas", forwardToReplicas.toString()))
    ), new Function<AsyncTask, AsyncTask>() {
      @Nullable @Override public AsyncTask apply(AsyncTask asyncTask) {
        return asyncTask.setIndex(indexName);
      }
    });
  }

  ListenableFuture<AsyncTask> clearSynonyms(final String indexName, Boolean forwardToReplicas) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.POST,
//        false,
//        Arrays.asList("1", "indexes", indexName, "synonyms", "clear"),
//        AsyncTask.class
//      ).setParameters(ImmutableMap.of("forwardToReplicas", forwardToReplicas.toString()))
//    ).thenApply(s -> s.setIndex(indexName));

    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.POST,
                    false,
                    Arrays.asList("1", "indexes", indexName, "synonyms", "clear"),
                    AsyncTask.class
            ).setParameters(ImmutableMap.of("forwardToReplicas", forwardToReplicas.toString()))
    ), new Function<AsyncTask, AsyncTask>() {
      @Nullable @Override public AsyncTask apply(AsyncTask asyncTask) {
        return asyncTask.setIndex(indexName);
      }
    });
  }

  ListenableFuture<SearchSynonymResult> searchSynonyms(String indexName, SynonymQuery query) {
    return httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.POST,
        false,
        Arrays.asList("1", "indexes", indexName, "synonyms", "search"),
        SearchSynonymResult.class
      ).setData(query)
    );
  }

  ListenableFuture<AsyncTask> batchSynonyms(final String indexName, List<AbstractSynonym> synonyms, Boolean forwardToReplicas, Boolean replaceExistingSynonyms) {
//    return httpClient.requestWithRetry(
//      new AlgoliaRequest<>(
//        HttpMethod.POST,
//        false,
//        Arrays.asList("1", "indexes", indexName, "synonyms", "batch"),
//        AsyncTask.class
//      )
//        .setParameters(ImmutableMap.of("forwardToReplicas", forwardToReplicas.toString(), "replaceExistingSynonyms", replaceExistingSynonyms.toString()))
//        .setData(synonyms)
//    ).thenApply(s -> s.setIndex(indexName));

    return Futures.transform(httpClient.requestWithRetry(
            new AlgoliaRequest<>(
                    HttpMethod.POST,
                    false,
                    Arrays.asList("1", "indexes", indexName, "synonyms", "batch"),
                    AsyncTask.class
            )
                    .setParameters(ImmutableMap.of("forwardToReplicas", forwardToReplicas.toString(), "replaceExistingSynonyms", replaceExistingSynonyms.toString()))
                    .setData(synonyms)
    ), new Function<AsyncTask, AsyncTask>() {
      @Nullable @Override public AsyncTask apply(AsyncTask asyncTask) {
        return asyncTask.setIndex(indexName);
      }
    });
  }

  ListenableFuture<AsyncTaskSingleIndex> partialUpdateObjects(final String indexName, List<Object> objects) {
    return Futures.transform(batch(
            indexName,
//      objects.stream().map(BatchPartialUpdateObjectOperation::new).collect(Collectors.toList())
            Lists.newArrayList(Iterables.transform(objects, new Function<Object, BatchOperation>() {
              @Nullable
              @Override
              public BatchOperation apply(@Nullable Object object) {
                return new BatchPartialUpdateObjectOperation<>(object);
              }
            }))
//    ).thenApply(s -> s.setIndex(indexName));
    ), new Function<AsyncTaskSingleIndex, AsyncTaskSingleIndex>() {
      @Override
      public AsyncTaskSingleIndex apply(AsyncTaskSingleIndex task) {
        return task.setIndex(indexName);
      }
    });
  }

  ListenableFuture<SearchFacetResult> searchFacet(String indexName, String facetName, String facetQuery, Query query) {
    query = query == null ? new Query() : query;
    query = query.addCustomParameter("facetQuery", facetQuery);

    return httpClient.requestWithRetry(
      new AlgoliaRequest<>(
        HttpMethod.POST,
        false,
        Arrays.asList("1", "indexes", indexName, "facets", facetName, "query"),
        SearchFacetResult.class
      ).setData(new Search(query))
    );
  }
}
