package com.codestack;

import com.codestack.base.Consumer;
import com.codestack.base.Message;
import com.codestack.base.MessageCondition;
import com.codestack.base.MessageQueue;

import java.util.*;
import java.util.concurrent.*;

public class InMemoryMessageQueue implements MessageQueue {

  private final ConcurrentMap<MessageCondition, List<DependentConsumer>> subscribers;
  private final Queue<Message> messages;
  private final int messagesSize;
  private final ExecutorService executorService;
  private final int maxTries = 3;
  private final List<Future> submittedTasks = new ArrayList<>();

  public InMemoryMessageQueue(int messagesSize, int maxWorkerThreads) {
    this.messagesSize = messagesSize;
    this.subscribers = new ConcurrentHashMap<>();
    this.messages = new ConcurrentLinkedQueue<>();
    this.executorService = Executors.newFixedThreadPool(maxWorkerThreads);
  }

  @Override
  public void notify(Message message) throws Exception {
    addIfQueueCanProcess(message);
    process();
  }

  private void addIfQueueCanProcess(Message message) throws Exception {
    if (messages.size() <= messagesSize) {
      messages.offer(message);
    } else {
      throw new Exception("Queue is full");
    }
  }

  private void process() {
    final Future<?> future =
        this.executorService.submit(
            () -> {
              final Message message = this.messages.poll();
              subscribers.forEach(
                  (condition, consumers) -> {
                    if (condition.test(message)) {
                      final Stack<DependentConsumer> consumersToProcess = new Stack<>();
                      consumers.forEach(consumersToProcess::push);

                      while (!consumersToProcess.isEmpty()) {
                        final DependentConsumer consumer = consumersToProcess.peek();
                        if (consumer.dependentOn.length != 0) {
                          boolean allProcessed = true;

                          for (Consumer dependency : consumer.dependentOn) {
                            final DependentConsumer dependentConsumer =
                                findConsumerIn(consumers, dependency);
                            if (dependentConsumer != null && !dependentConsumer.processed) {
                              allProcessed = false;
                              if (consumersToProcess.indexOf(dependentConsumer) != -1) {
                                consumersToProcess.remove(dependentConsumer);
                              }

                              consumersToProcess.push(dependentConsumer);
                            }
                          }

                          if (allProcessed) {
                            consumer.tryProcess(message);
                            consumersToProcess.pop();
                          }
                        } else {
                          consumer.tryProcess(message);
                          consumersToProcess.pop();
                        }
                      }
                    }
                  });
            });

    submittedTasks.add(future);
  }

  private DependentConsumer findConsumerIn(List<DependentConsumer> consumers, Consumer consumer) {
    final int index = consumers.indexOf(new DependentConsumer(consumer, null));
    if (index != -1) {
      return consumers.get(index);
    } else {
      return null;
    }
  }

  @Override
  public void subscribe(
      MessageCondition messageCondition, Consumer consumer, Consumer... dependentOn) {
    final DependentConsumer dependentConsumer = new DependentConsumer(consumer, dependentOn);
    final List<DependentConsumer> dependentConsumers =
        subscribers.putIfAbsent(
            messageCondition, new CopyOnWriteArrayList<>(Collections.singleton(dependentConsumer)));
    if (dependentConsumers != null) {
      checkForCircularDependency(dependentConsumers, dependentConsumer);
      dependentConsumers.add(dependentConsumer);
    }
  }

  private synchronized void checkForCircularDependency(
      List<DependentConsumer> dependentConsumers, DependentConsumer dependentConsumer) {
    final Stack<DependentConsumer> allConsumers = new Stack<>();
    allConsumers.push(dependentConsumer);

    while (!allConsumers.isEmpty()) {
      final DependentConsumer poppedConsumer = allConsumers.pop();
      for (int i = 0; i < poppedConsumer.dependentOn.length; i++) {
        final DependentConsumer consumerIn =
            findConsumerIn(dependentConsumers, poppedConsumer.dependentOn[i]);
        if (consumerIn != null) {
          allConsumers.push(consumerIn);
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "No consumer currently subscribed with name: %s",
                  poppedConsumer.dependentOn[i].getName()));
        }
      }
    }
  }

  public void waitUntilAllPendingTaskCompletes() throws Exception {
    for (int i = 0; i < submittedTasks.size(); i++) {
      submittedTasks.get(i).get();
    }
  }

  private class DependentConsumer {
    private final Consumer consumer;
    private final Consumer[] dependentOn;
    private boolean processed;
    private int totalTries;

    public DependentConsumer(Consumer consumer, Consumer[] dependentOn) {
      this.consumer = consumer;
      this.dependentOn = dependentOn;
      this.processed = false;
      this.totalTries = 0;
    }

    public void tryProcess(Message message) {
      while (!processed) {
        try {
          consumer.process(message);
          processed = true;
        } catch (Exception e) {
          totalTries++;
          if (totalTries >= maxTries) {
            break;
          }
        }
      }
    }

    @Override
    public boolean equals(Object a) {
      if (this == a) return true;
      if (a == null || getClass() != a.getClass()) return false;
      DependentConsumer that = (DependentConsumer) a;
      return Objects.equals(consumer, that.consumer);
    }

    @Override
    public int hashCode() {
      return Objects.hash(consumer);
    }
  }
}
