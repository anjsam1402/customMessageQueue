package com.codestack;

import com.codestack.base.Consumer;
import com.codestack.base.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JsonConsumer implements Consumer {

  private final String name;
  private final List<Message> consumerMessages = new ArrayList();

  public JsonConsumer(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void process(Message message) throws Exception {
    consumerMessages.add(message);
    System.out.println(String.format("Consumer: %s, Message: %s", name, message.serialize()));
  }

  public List<Message> getConsumerMessages() {
    return consumerMessages;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    JsonConsumer that = (JsonConsumer) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
