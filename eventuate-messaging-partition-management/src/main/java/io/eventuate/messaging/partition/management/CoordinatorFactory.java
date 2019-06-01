package io.eventuate.messaging.partition.management;

import java.util.Set;
import java.util.function.Consumer;

public interface CoordinatorFactory {
  Coordinator makeCoordinator(String subscriberId,
                              Set<String> channels,
                              String subscriptionId,
                              Consumer<Assignment> assignmentUpdatedCallback,
                              String lockId,
                              Runnable leaderSelected,
                              Runnable leaderRemoved);
}
