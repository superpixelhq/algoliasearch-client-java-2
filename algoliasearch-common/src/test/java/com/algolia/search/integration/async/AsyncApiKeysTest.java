package com.algolia.search.integration.async;

import com.algolia.search.AlgoliaObject;
import com.algolia.search.AsyncAlgoliaIntegrationTest;
import com.algolia.search.AsyncIndex;
import com.algolia.search.inputs.BatchOperation;
import com.algolia.search.inputs.batch.BatchDeleteIndexOperation;
import com.algolia.search.objects.ApiKey;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

abstract public class AsyncApiKeysTest extends AsyncAlgoliaIntegrationTest {

  private static List<String> indicesNames = Arrays.asList(
    "index1",
    "index2"
  );

  @Before
  @After
  public void cleanUp() throws Exception {
    List<BatchOperation> clean = indicesNames.stream().map(BatchDeleteIndexOperation::new).collect(Collectors.toList());
    waitForCompletion(client.batch(clean));
  }

  private void waitForKeyPresent(AsyncIndex<AlgoliaObject> index, String description) throws Exception {
    for (int i = 0; i < 100; i++) {
      Thread.sleep(1000);
      ListenableFuture<List<ApiKey>> apiKeys = index == null ? client.listKeys() : index.listKeys();
      boolean found = apiKeys.get(WAIT_TIME_IN_SECONDS, SECONDS).stream().map(ApiKey::getDescription).anyMatch(k -> k.equals(description));
      if (found) {
        return;
      }
    }

    //will fail
    futureAssertThat(client.listKeys()).extracting("description").contains(description);
  }

  private void waitForKeyNotPresent(AsyncIndex<AlgoliaObject> index, String description) throws Exception {
    for (int i = 0; i < 100; i++) {
      Thread.sleep(1000);
      ListenableFuture<List<ApiKey>> apiKeys = index == null ? client.listKeys() : index.listKeys();
      boolean found = apiKeys.get(WAIT_TIME_IN_SECONDS, SECONDS).stream().map(ApiKey::getDescription).anyMatch(k -> k.equals(description));
      if (!found) {
        return;
      }
    }

    //will fail
    futureAssertThat(client.listKeys()).extracting("description").doesNotContain(description);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void manageKeys() throws Exception {
    //Fill index
    waitForCompletion(client.initIndex("index1", AlgoliaObject.class).addObject(new AlgoliaObject("1", 1)));

    ApiKey apiKey = new ApiKey()
      .setDescription("toto" + System.currentTimeMillis())
      .setIndexes(Collections.singletonList("index1"));

    String keyName = client.addKey(apiKey).get().getKey();
    assertThat(keyName).isNotNull();

    waitForKeyPresent(null, apiKey.getDescription());

    apiKey = apiKey.setDescription("toto2" + System.currentTimeMillis());
    client.updateKey(keyName, apiKey).get();

    waitForKeyPresent(null, apiKey.getDescription());

    assertThat(client.getKey(keyName).get().get().getDescription()).isEqualTo(apiKey.getDescription());

    client.deleteKey(keyName).get();

    waitForKeyNotPresent(null, apiKey.getDescription());
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void manageKeysForIndex() throws Exception {
    //Fill index
    AsyncIndex<AlgoliaObject> index = client.initIndex("index2", AlgoliaObject.class);
    waitForCompletion(index.addObject(new AlgoliaObject("1", 1)));

    ApiKey apiKey = new ApiKey()
      .setDescription("toto" + System.currentTimeMillis());

    String keyName = index.addKey(apiKey).get().getKey();
    assertThat(keyName).isNotNull();

    waitForKeyPresent(index, apiKey.getDescription());

    apiKey = apiKey.setDescription("toto2" + System.currentTimeMillis());
    index.updateKey(keyName, apiKey).get();

    waitForKeyPresent(index, apiKey.getDescription());

    assertThat(index.getKey(keyName).get().get().getDescription()).isEqualTo(apiKey.getDescription());

    index.deleteKey(keyName).get();

    waitForKeyNotPresent(index, apiKey.getDescription());
  }

}
