package chatroom.util;

public record ChatServerIdentity (int port){

    public int compare(ChatServerIdentity other){
        return this.port - other.port;
    }
}
