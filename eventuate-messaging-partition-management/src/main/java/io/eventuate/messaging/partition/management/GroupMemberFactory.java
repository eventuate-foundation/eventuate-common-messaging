package io.eventuate.messaging.partition.management;

public interface GroupMemberFactory {
  GroupMember create(String groupId, String memberId);
}
