package io.eventuate.common.messaging;

import java.util.function.Consumer;

public interface AssignmentListenerFactory {
  AssignmentListener create(String groupId, String memberId, Consumer<Assignment> assignmentUpdatedCallback);
}
