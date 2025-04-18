package message;

import client.User;

import java.time.LocalDateTime;
import java.util.Map;

public class Message {

    private String message;
    //Can just be string in implementation, using class to be generic
    private User user;
    //Just for display purposes, not used in logic
    private LocalDateTime timestamp;
    //Vector clock when message was sent
    private VectorClock vectorClock;

    public Message(String message, User user, LocalDateTime timestamp, VectorClock vectorClock) {
        this.message = message;
        this.user = user;
        this.timestamp = timestamp;
        this.vectorClock = vectorClock;
    }

    public User getUser() {
        return user;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }

    //Compares two messages in regards to their message order
    //Returns -1 if this message is before the other, 1 if this message is after the other, and 0 if they are the same
    public int compare(Message other, ChatServerIdentity self, ChatServerIdentity sender) {
        //Compare the vector clocks
        int vectorClockComparison = vectorClock.compare(other.vectorClock);
        if (vectorClockComparison != 0) {
            return vectorClockComparison;
        }
        //If the messages are concurrent, compare chat server identities as tiebreaker
        return self.compare(sender);
    }
}
