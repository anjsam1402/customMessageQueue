package com.codestack.base;

public interface MessageQueue {
  void notify(Message message) throws Exception;

  void subscribe(MessageCondition messageCondition, Consumer consumer, Consumer... dependentOn);
}
