package io.eventuate.messaging.partitionmanagement.tests;

import io.eventuate.messaging.partitionmanagement.CommonMessageConsumer;
import io.eventuate.util.test.async.Eventually;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class AbstractMessagingTest {
  protected static class EventuallyConfig {
    public final int iterations;
    public final int timeout;
    public final TimeUnit timeUnit;

    public EventuallyConfig(int iterations, int timeout, TimeUnit timeUnit) {
      this.iterations = iterations;
      this.timeout = timeout;
      this.timeUnit = timeUnit;
    }
  }

  protected static class TestSubscription {
    private CommonMessageConsumer messageConsumer;
    private ConcurrentLinkedQueue<Integer> messageQueue;
    private Set<Integer> currentPartitions;
    private boolean leader;

    public TestSubscription(CommonMessageConsumer messageConsumer, ConcurrentLinkedQueue<Integer> messageQueue) {
      this.messageConsumer = messageConsumer;
      this.messageQueue = messageQueue;
    }

    public CommonMessageConsumer getConsumer() {
      return messageConsumer;
    }

    public ConcurrentLinkedQueue<Integer> getMessageQueue() {
      return messageQueue;
    }

    public Set<Integer> getCurrentPartitions() {
      return currentPartitions;
    }

    public void setCurrentPartitions(Set<Integer> currentPartitions) {
      this.currentPartitions = currentPartitions;
    }

    public void clearMessages() {
      messageQueue.clear();
    }

    public void close() {
      messageConsumer.close();
      messageQueue.clear();
    }

    public boolean isLeader() {
      return leader;
    }

    public void setLeader(boolean leader) {
      this.leader = leader;
    }
  }

  protected static final int DEFAULT_PARTITION_COUNT = 2;
  protected static final int DEFAULT_MESSAGE_COUNT = 100;
  protected static final EventuallyConfig EVENTUALLY_CONFIG = new EventuallyConfig(200, 400, TimeUnit.MILLISECONDS);

  protected AtomicInteger consumerIdCounter;
  protected AtomicInteger subscriptionIdCounter;
  protected AtomicInteger messageIdCounter;

  protected Supplier<String> consumerIdSupplier = () -> "consumer" + consumerIdCounter.getAndIncrement();
  protected Supplier<String> subscriptionIdSupplier = () -> "subscription" + subscriptionIdCounter.getAndIncrement();

  protected Supplier<String> subscriberIdSupplier = () -> "subscriber" + System.nanoTime();
  protected Supplier<String> channelIdSupplier = () -> "channel" + System.nanoTime();

  protected String destination;
  protected String subscriberId;

  @BeforeEach
  public void init() {
    consumerIdCounter = new AtomicInteger(1);
    subscriptionIdCounter = new AtomicInteger(1);
    messageIdCounter = new AtomicInteger(1);

    destination = channelIdSupplier.get();
    subscriberId = subscriberIdSupplier.get();
  }

  @Test
  public void test1Consumer2Partitions() throws Exception {
    TestSubscription subscription = subscribe();

    assertSubscriptionPartitionsBalanced(Arrays.asList(subscription));

    sendMessages();

    assertMessagesConsumed(subscription);
  }

  @Test
  public void test2Consumers2Partitions() {
    TestSubscription subscription1 = subscribe();
    TestSubscription subscription2 = subscribe();

    assertSubscriptionPartitionsBalanced(Arrays.asList(subscription1, subscription2));

    sendMessages();

    assertMessagesConsumed(Arrays.asList(subscription1, subscription2));
  }


  @Test
  public void test1Consumer2PartitionsThenAddedConsumer() {
    TestSubscription testSubscription1 = subscribe();

    assertSubscriptionPartitionsBalanced(Arrays.asList(testSubscription1));

    sendMessages();

    assertMessagesConsumed(testSubscription1);

    testSubscription1.clearMessages();
    TestSubscription testSubscription2 = subscribe();

    assertSubscriptionPartitionsBalanced(Arrays.asList(testSubscription1, testSubscription2));

    sendMessages();

    assertMessagesConsumed(Arrays.asList(testSubscription1, testSubscription2));
  }

  @Test
  public void test2Consumers2PartitionsThenRemovedConsumer() {
    TestSubscription testSubscription1 = subscribe();
    TestSubscription testSubscription2 = subscribe();

    assertSubscriptionPartitionsBalanced(Arrays.asList(testSubscription1, testSubscription2));

    sendMessages();

    assertMessagesConsumed(Arrays.asList(testSubscription1, testSubscription2));

    testSubscription1.clearMessages();
    testSubscription2.close();

    assertSubscriptionPartitionsBalanced(Arrays.asList(testSubscription1));

    sendMessages();

    assertMessagesConsumed(testSubscription1);
  }

  @Test
  public void test5Consumers9PartitionsThenRemoved2LeaderConsumersAndAdded3Consumers() {
    int partitionCount = 9;
    int initialConsumers = 5;
    int removedConsumers = 2;
    int addedConsumers = 3;

    LinkedList<TestSubscription> testSubscriptions = createConsumersAndSubscribe(initialConsumers, partitionCount);

    assertSubscriptionPartitionsBalanced(testSubscriptions, partitionCount);

    sendMessages(partitionCount);

    assertMessagesConsumed(testSubscriptions);

    closeAndRemoveLeaderSubscribers(testSubscriptions, removedConsumers);

    testSubscriptions.forEach(TestSubscription::clearMessages);

    testSubscriptions.addAll(createConsumersAndSubscribe(addedConsumers, partitionCount));

    assertSubscriptionPartitionsBalanced(testSubscriptions, partitionCount);

    sendMessages(partitionCount);

    assertMessagesConsumed(testSubscriptions);
  }

  @Test
  public void testReassignment() {
    runReassignmentIteration();
    runReassignmentIteration();
  }

  private void runReassignmentIteration() {
    TestSubscription testSubscription1 = subscribe();

    assertSubscriptionPartitionsBalanced(Arrays.asList(testSubscription1));

    sendMessages();

    try {
      assertMessagesConsumed(testSubscription1);
    } catch (Throwable t){
      testSubscription1.close();
      throw t;
    }

    testSubscription1.clearMessages();
    TestSubscription testSubscription2 = subscribe();

    assertSubscriptionPartitionsBalanced(Arrays.asList(testSubscription1, testSubscription2));

    sendMessages();

    try {
      assertMessagesConsumed(Arrays.asList(testSubscription1, testSubscription2));
    } finally {
      testSubscription1.close();
      testSubscription2.close();
    }
  }

  protected TestSubscription subscribe() {
    return subscribe(DEFAULT_PARTITION_COUNT);
  }

  protected abstract TestSubscription subscribe(int partitionCount);

  protected void assertSubscriptionPartitionsBalanced(List<TestSubscription> subscriptions) {
    assertSubscriptionPartitionsBalanced(subscriptions, DEFAULT_PARTITION_COUNT);
  }

  protected void sendMessages() {
    sendMessages(DEFAULT_MESSAGE_COUNT, DEFAULT_PARTITION_COUNT);
  }

  protected void sendMessages(int partitions) {
    sendMessages(DEFAULT_MESSAGE_COUNT, partitions);
  }

  protected abstract void sendMessages(int messageCount, int partitions);

  protected void assertSubscriptionPartitionsBalanced(List<TestSubscription> subscriptions, int expectedPartitionCount) {
    Eventually.eventually(EVENTUALLY_CONFIG.iterations,
            EVENTUALLY_CONFIG.timeout,
            EVENTUALLY_CONFIG.timeUnit,
            () -> {
              Assertions.assertTrue(subscriptions
                      .stream()
                      .noneMatch(testSubscription -> testSubscription.getCurrentPartitions().isEmpty()), "not all subscriptions have assigned partitions");

              List<Integer> allPartitions = subscriptions
                      .stream()
                      .map(TestSubscription::getCurrentPartitions)
                      .flatMap(Collection::stream)
                      .collect(Collectors.toList());

              Set<Integer> uniquePartitions = new HashSet<>(allPartitions);

              Assertions.assertEquals(allPartitions.size(), uniquePartitions.size(), "partitions are not unique across subscriptions");
              Assertions.assertEquals(expectedPartitionCount, uniquePartitions.size(), "actual partition count not equals to expected partition count");
            });
  }

  protected void assertMessagesConsumed(TestSubscription testSubscription) {
    assertMessagesConsumed(testSubscription, DEFAULT_MESSAGE_COUNT);
  }

  protected void assertMessagesConsumed(TestSubscription testSubscription, int messageCount) {
    assertMessagesConsumed(Arrays.asList(testSubscription), messageCount);
  }

  protected void assertMessagesConsumed(List<TestSubscription> testSubscriptions) {
    assertMessagesConsumed(testSubscriptions, DEFAULT_MESSAGE_COUNT);
  }

  protected void assertMessagesConsumed(List<TestSubscription> testSubscriptions, int messageCount) {
    Eventually.eventually(EVENTUALLY_CONFIG.iterations,
            EVENTUALLY_CONFIG.timeout,
            EVENTUALLY_CONFIG.timeUnit,
            () -> {

              List<TestSubscription> emptySubscriptions = testSubscriptions
                      .stream()
                      .filter(testSubscription -> testSubscription.getMessageQueue().isEmpty())
                      .collect(Collectors.toList());

              Assertions.assertTrue(emptySubscriptions.isEmpty(), "Some subscriptions are empty");

              Assertions.assertEquals((long) messageCount,
                      (long) testSubscriptions
                              .stream()
                              .map(testSubscription -> testSubscription.getMessageQueue().size())
                              .reduce((a, b) -> a + b)
                              .orElse(0),
                      "sent messages not equal to consumed messages");
            });
  }

  protected LinkedList<TestSubscription> createConsumersAndSubscribe(int consumerCount, int partitionCount) {

    LinkedList<TestSubscription> subscriptions = new LinkedList<>();

    for (int i = 0; i < consumerCount; i++) {
      subscriptions.add(subscribe(partitionCount));
    }

    return subscriptions;
  }

  protected void closeAndRemoveLeaderSubscribers(LinkedList<TestSubscription> subscriptions, int count) {
    for (int i = 0; i < count; i ++) {
      Eventually.eventually(() -> {
        for (int j = 0; j < subscriptions.size(); j++) {
          if (subscriptions.get(j).isLeader()) {
            subscriptions.remove(j).close();
            return;
          }
        }
        throw new RuntimeException();
      });
    }
  }
}
