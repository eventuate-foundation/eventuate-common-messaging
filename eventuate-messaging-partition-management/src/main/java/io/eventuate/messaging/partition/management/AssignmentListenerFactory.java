package io.eventuate.messaging.partition.management;

import java.util.function.Consumer;

public interface AssignmentListenerFactory {
  AssignmentListener create(String groupId, String memberId, Consumer<Assignment> assignmentUpdatedCallback);
}
