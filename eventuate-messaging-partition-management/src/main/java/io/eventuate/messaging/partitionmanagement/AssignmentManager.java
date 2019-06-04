package io.eventuate.messaging.partitionmanagement;

public interface AssignmentManager {
  void initializeAssignment(String groupId, String memberId, Assignment assignment);
  Assignment readAssignment(String groupId, String memberId);
  void saveAssignment(String groupId, String memberId, Assignment assignment);
}
