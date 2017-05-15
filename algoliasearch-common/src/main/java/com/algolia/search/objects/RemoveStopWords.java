package com.algolia.search.objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Joiner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@JsonDeserialize(using = RemoveStopWordsDeserializer.class)
@JsonSerialize(using = RemoveStopWordsSerializer.class)
public abstract class RemoveStopWords {

  public static RemoveStopWords of(Boolean bool) {
    return new RemoveStopWordsBoolean(bool);
  }

  public static RemoveStopWords of(List<String> strings) {
    return new RemoveStopWordsListString(strings);
  }

  @JsonIgnore
  abstract Object getInsideValue();

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RemoveStopWords that = (RemoveStopWords) o;

    return getInsideValue() != null ? getInsideValue().equals(that.getInsideValue()) : that.getInsideValue() == null;
  }

  @Override
  public int hashCode() {
    return getInsideValue() != null ? getInsideValue().hashCode() : 0;
  }
}

class RemoveStopWordsBoolean extends RemoveStopWords {

  private boolean insideValue;

  RemoveStopWordsBoolean(boolean insideValue) {
    this.insideValue = insideValue;
  }

  @Override
  Object getInsideValue() {
    return insideValue;
  }
}

class RemoveStopWordsListString extends RemoveStopWords {

  private List<String> insideValue;

  RemoveStopWordsListString(List<String> insideValue) {
    this.insideValue = insideValue;
  }

  @Override
  Object getInsideValue() {
    return insideValue;
  }
}

class RemoveStopWordsDeserializer extends JsonDeserializer<RemoveStopWords> {

  @Override
  public RemoveStopWords deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
    JsonToken currentToken = p.getCurrentToken();
    if (currentToken.equals(JsonToken.VALUE_STRING)) {
      return RemoveStopWords.of(Arrays.asList(p.getValueAsString().split(",")));
    }

    return RemoveStopWords.of(p.getBooleanValue());
  }
}

class RemoveStopWordsSerializer extends JsonSerializer<RemoveStopWords> {

  @SuppressWarnings("unchecked")
  @Override
  public void serialize(RemoveStopWords value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
    if (value instanceof RemoveStopWordsBoolean) {
      gen.writeBoolean((Boolean) value.getInsideValue());
    } else if (value instanceof RemoveStopWordsListString) {
      List<String> list = (List<String>) value.getInsideValue();
      gen.writeString(Joiner.on(",").join(list));
    }
  }
}