package com.codestack;

import com.codestack.base.Message;

import java.util.Objects;

public class JsonMessage implements Message {

  private String serializedJson;

  public JsonMessage(String serializedJson) {
    this.serializedJson = serializedJson;
  }

  @Override
  public String serialize() {
    return serializedJson;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    JsonMessage that = (JsonMessage) o;
    return Objects.equals(serializedJson, that.serializedJson);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serializedJson);
  }
}
