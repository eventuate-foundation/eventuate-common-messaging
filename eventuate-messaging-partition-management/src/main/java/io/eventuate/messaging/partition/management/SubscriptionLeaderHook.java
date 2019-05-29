package io.eventuate.messaging.partition.management;

public interface SubscriptionLeaderHook {
  void leaderUpdated(Boolean leader, String subscriptionId);
}
