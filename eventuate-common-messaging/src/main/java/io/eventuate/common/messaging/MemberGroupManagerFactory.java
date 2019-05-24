package io.eventuate.common.messaging;

import java.util.Set;
import java.util.function.Consumer;

public interface MemberGroupManagerFactory {
  MemberGroupManager create(String groupId, String memberId, Consumer<Set<String>> groupMembersUpdatedCallback);
}
