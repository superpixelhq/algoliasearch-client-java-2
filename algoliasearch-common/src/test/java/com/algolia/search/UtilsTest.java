package com.algolia.search;

import com.algolia.search.exceptions.AlgoliaException;
import com.algolia.search.objects.Query;
import org.junit.Test;
import net.javacrumbs.futureconverter.java8guava.FutureConverter;


import static org.assertj.core.api.Assertions.assertThat;

public class UtilsTest {

  @Test
  public void secureKeys() throws AlgoliaException {
    assertThat(
      Utils.generateSecuredApiKey("182634d8894831d5dbce3b3185c50881", new Query().setTagFilters("(public,user1)"), null)
    ).isEqualTo("MDZkNWNjNDY4M2MzMDA0NmUyNmNkZjY5OTMzYjVlNmVlMTk1NTEwMGNmNTVjZmJhMmIwOTIzYjdjMTk2NTFiMnRhZ0ZpbHRlcnM9JTI4cHVibGljJTJDdXNlcjElMjk=");

    assertThat(
      Utils.generateSecuredApiKey("182634d8894831d5dbce3b3185c50881", new Query().setTagFilters("(public,user1)").setUserToken("42"), null)
    ).isEqualTo("OGYwN2NlNTdlOGM2ZmM4MjA5NGM0ZmYwNTk3MDBkNzMzZjQ0MDI3MWZjNTNjM2Y3YTAzMWM4NTBkMzRiNTM5YnRhZ0ZpbHRlcnM9JTI4cHVibGljJTJDdXNlcjElMjkmdXNlclRva2VuPTQy");
  }

  @Test
  public void completeExceptionally() {
    assertThat(FutureConverter.toCompletableFuture(Utils.completeExceptionally(new NullPointerException()))).isCompletedExceptionally();
  }

}