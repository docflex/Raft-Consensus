package learning.model;

/** The three possible roles a Raft server can hold during the consensus protocol. */
public enum ServerRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
