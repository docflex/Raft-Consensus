package learning.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tracks the current cluster membership.
 * Provides majority calculation and member management for single-server changes.
 */
public class ClusterConfig {

    private final Set<ServerID> members;

    public ClusterConfig(ServerID self, List<ServerID> peers) {
        this.members = new HashSet<>();
        this.members.add(self);
        this.members.addAll(peers);
    }

    public ClusterConfig(Set<ServerID> members) {
        this.members = new HashSet<>(members);
    }

    public Set<ServerID> getMembers() {
        return Collections.unmodifiableSet(members);
    }

    public Set<ServerID> getPeers(ServerID self) {
        Set<ServerID> peers = new HashSet<>(members);
        peers.remove(self);
        return peers;
    }

    public int size() {
        return members.size();
    }

    public int majority() {
        return size() / 2 + 1;
    }

    public boolean contains(ServerID id) {
        return members.contains(id);
    }

    public ClusterConfig withAdded(ServerID id) {
        Set<ServerID> newMembers = new HashSet<>(members);
        newMembers.add(id);
        return new ClusterConfig(newMembers);
    }

    public ClusterConfig withRemoved(ServerID id) {
        Set<ServerID> newMembers = new HashSet<>(members);
        newMembers.remove(id);
        return new ClusterConfig(newMembers);
    }
}
