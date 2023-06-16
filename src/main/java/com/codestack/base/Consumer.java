package com.codestack.base;

public interface Consumer {
  String getName();

  void process(Message message) throws Exception;
}
