package com.codestack;

import com.codestack.base.Message;
import com.codestack.base.MessageQueue;
import com.codestack.base.Producer;

public class JsonProducer implements Producer {
  private final MessageQueue messageQueue;

  public JsonProducer(MessageQueue messageQueue) {
    this.messageQueue = messageQueue;
  }

  @Override
  public void produce(Message message) {
    while (true) {
      try {
        messageQueue.notify(message);
        break;
      } catch (Exception e) {

      }
    }
  }
}
