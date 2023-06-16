package com.codestack;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

import com.codestack.base.Consumer;
import com.codestack.base.Message;
import com.codestack.base.MessageCondition;
import com.codestack.base.Producer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Before;
import org.junit.Test;

public class InMemoryMessageQueueTest {
  private InMemoryMessageQueue inMemoryMessageQueue;
  private Producer producer;
  private List<String> messageList;

  @Before
  public void setUp() {
    inMemoryMessageQueue = new InMemoryMessageQueue(10, 2);
    producer = new JsonProducer(inMemoryMessageQueue);
    messageList = new CopyOnWriteArrayList<>();
  }

  @Test
  public void testSuccessCaseOfSendingMessageFromProducerToConsumer() throws Exception {
    // having
    final TestConsumer consumer = new TestConsumer("A", messageList);
    final MessageCondition anyMessageWithTrue = new PatternMessageCondition(".*true.*");
    final Message message = new JsonMessage("{'value': 'true'}");
    inMemoryMessageQueue.subscribe(anyMessageWithTrue, consumer);

    // when
    producer.produce(message);
    inMemoryMessageQueue.waitUntilAllPendingTaskCompletes();
    producer.produce(message);
    inMemoryMessageQueue.waitUntilAllPendingTaskCompletes();

    // then
    assertTrue(messageList.contains("A : {'value': 'true'}"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenCircularDependency() throws Exception {
    // having
    final JsonConsumer consumerA = new JsonConsumer("A");
    final JsonConsumer consumerB = new JsonConsumer("B");
    final MessageCondition anyMessageWithTrue = new PatternMessageCondition(".*true.*");
    inMemoryMessageQueue.subscribe(anyMessageWithTrue, consumerA, consumerB);
    inMemoryMessageQueue.subscribe(anyMessageWithTrue, consumerB, consumerA);
  }

  @Test
  public void shouldRetryThreeTimesWhenProcessingFails() throws Exception {
    // having
    final TestConsumer consumer = mock(TestConsumer.class);
    doThrow(Exception.class).when(consumer).process(anyObject());

    final MessageCondition anyMessageWithTrue = new PatternMessageCondition(".*true.*");
    final Message message = new JsonMessage("{'value': 'true'}");
    inMemoryMessageQueue.subscribe(anyMessageWithTrue, consumer);

    // when
    producer.produce(message);
    inMemoryMessageQueue.waitUntilAllPendingTaskCompletes();

    // then
    verify(consumer, times(3)).process(any());
  }

  @Test
  public void shouldServeMessageFromProducerToMultipleConsumer() throws Exception {
    // having
    final TestConsumer consumerA = new TestConsumer("A", messageList);
    final TestConsumer consumerB = new TestConsumer("B", messageList);
    final TestConsumer consumerC = new TestConsumer("C", messageList);
    final TestConsumer consumerD = new TestConsumer("D", messageList);

    final MessageCondition anyMessageWithTrue = new PatternMessageCondition(".*true.*");
    final Message message = new JsonMessage("{'value': 'true'}");
    inMemoryMessageQueue.subscribe(anyMessageWithTrue, consumerA);
    inMemoryMessageQueue.subscribe(anyMessageWithTrue, consumerB);
    inMemoryMessageQueue.subscribe(anyMessageWithTrue, consumerC);
    inMemoryMessageQueue.subscribe(anyMessageWithTrue, consumerD);

    // when
    producer.produce(message);
    inMemoryMessageQueue.waitUntilAllPendingTaskCompletes();

    // then
    assertThat(
        messageList,
        containsInAnyOrder(
            "A: {'value': 'true'}",
            "B: {'value': 'true'}",
            "C: {'value': 'true'}",
            "D: {'value': 'true'}"));
  }

  @Test
  public void shouldOnlyServeMessageToRelevantConsumer() throws Exception {
    // having
    final TestConsumer consumerA = new TestConsumer("A", messageList);
    final TestConsumer consumerB = new TestConsumer("B", messageList);
    final TestConsumer consumerC = new TestConsumer("C", messageList);
    final TestConsumer consumerD = new TestConsumer("D", messageList);

    final MessageCondition anyMessageWithTrue = new PatternMessageCondition(".*true.*");
    final MessageCondition notMatchingCondition = new PatternMessageCondition(".*notMatching.*");
    final Message message = new JsonMessage("{'value': 'true'}");
    inMemoryMessageQueue.subscribe(anyMessageWithTrue, consumerA);
    inMemoryMessageQueue.subscribe(notMatchingCondition, consumerB);
    inMemoryMessageQueue.subscribe(anyMessageWithTrue, consumerC);
    inMemoryMessageQueue.subscribe(notMatchingCondition, consumerD);

    // when
    producer.produce(message);
    inMemoryMessageQueue.waitUntilAllPendingTaskCompletes();

    // then
    assertThat(messageList, containsInAnyOrder("A: {'value': 'true'}", "C: {'value': 'true'}"));
  }

  @Test
  public void shouldServeMessageFromProducerToConsumersInOrderOfDependency() throws Exception {
    // having
    final TestConsumer consumerA = new TestConsumer("A", messageList);
    final TestConsumer consumerB = new TestConsumer("B", messageList);
    final TestConsumer consumerC = new TestConsumer("C", messageList);

    final MessageCondition anyMessageWithTrue = new PatternMessageCondition(".*true.*");
    final Message message = new JsonMessage("{'value': 'true'}");
    inMemoryMessageQueue.subscribe(anyMessageWithTrue, consumerA, consumerC);
    inMemoryMessageQueue.subscribe(anyMessageWithTrue, consumerB);
    inMemoryMessageQueue.subscribe(anyMessageWithTrue, consumerC, consumerB);

    // when
    producer.produce(message);
    inMemoryMessageQueue.waitUntilAllPendingTaskCompletes();

    // then
    assertThat(
        messageList,
        contains("B: {'value': 'true'}", "C: {'value': 'true'}", "A: {'value': 'true'}"));
  }

  private class TestConsumer implements Consumer {

    private final String name;
    private final List<String> logger;

    private TestConsumer(String name, List<String> logger) {
      this.name = name;
      this.logger = logger;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void process(Message message) throws Exception {
      logger.add(String.format("%s : %s", name, message.serialize()));
    }
  }
}
