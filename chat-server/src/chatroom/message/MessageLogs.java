package chatroom.message;

import chatroom.util.ChatServerIdentity;

import java.util.ArrayList;
import java.util.List;

public class MessageLogs {

    private List<Message> messages;

    public MessageLogs() {
        this.messages = new ArrayList<Message>();
    }

    public void addMessage(Message message, ChatServerIdentity self, ChatServerIdentity sender) {
        messages.add(message);
        messages.sort((m1,m2) -> m1.compare(m2, self, sender));
    }
}
