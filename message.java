import java.nio.ByteBuffer;
import java.io.DataInputStream;
import java.io.IOException;

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
    
    public byte[] getPayload() {
        return payload;
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 1 + payload.length);
        buffer.putInt(length);
        buffer.put((byte)type.ordinal());
        buffer.put(payload);
        return buffer.array();
    }
    
    public static Message read (DataInputStream in) throws IOException {
        int length = in.readInt();
        byte typeByte = in.readByte();
        MessageType type = MessageType.values()[typeByte];
        byte[] payload = new byte[length - 1];
        in.readFully(payload);
        return new Message(length, type, payload);
    }
    
    public static Message choke() {
        return new Message(1, MessageType.CHOKE, new byte[0]);
    }

    public static Message have(int pieceIndex) {
        return new Message(5, MessageType.HAVE, ByteBuffer.allocate(4).putInt(pieceIndex).array());
    }

    public static Message bitfield(byte[] bitfield) {
        return new Message(1 + bitfield.length, MessageType.BITFIELD, bitfield);
    }

    public static Message request(int pieceIndex) {
        return new Message(5, MessageType.REQUEST, ByteBuffer.allocate(4).putInt(pieceIndex).array());
    }

    public static Message piece(int pieceIndex, byte[] data) {
        return new Message(5 + data.length, MessageType.PIECE, ByteBuffer.allocate(4 + data.length).putInt(pieceIndex).put(data).array());
    }

    public static Message unchoke() {
        return new Message(1, MessageType.UNCHOKE, new byte[0]);
    }

    public static Message interested() {
        return new Message(1, MessageType.INTERESTED, new byte[0]);
    }

    public static Message notInterested() {
        return new Message(1, MessageType.NOT_INTERESTED, new byte[0]);
    }
}