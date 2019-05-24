package io.eventuate.common.messaging;

public interface GroupMemberFactory {
  GroupMember create(String groupId, String memberId);
}
