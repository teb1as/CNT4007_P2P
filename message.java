import messageTypes.MessageType;

class Message {
    private int length;
    private MessageType type;
    private byte[] payload;

    public Message(int length, MessageType type, byte[] payload) {
        this.length = length;
        this.type = type;
        this.payload = payload;
    }

    public MessageType getType() {
        return type;
    }

    public int getLength() {
        return length;
    }
}