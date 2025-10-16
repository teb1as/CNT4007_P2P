import java.io.*;
import java.nio.ByteBuffer;

class Handshake {
    private static final String HANDSHAKE_HEADER = "P2PFILESHARING";
    private int peerID;
    
    public Handshake(int peerID) {
        this.peerID = peerID;
    }
    
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.put(HANDSHAKE_HEADER.getBytes());
        buffer.put(new byte[10]);
        buffer.putInt(peerID);
        return buffer.array();
    }
    
    public static Handshake read(DataInputStream in) throws IOException {
        byte[] header = new byte[18];
        in.readFully(header);
        
        byte[] zeros = new byte[10];
        in.readFully(zeros);
        
        int peerID = in.readInt();
        
        System.out.println("handshake received from peer " + peerID);
        return new Handshake(peerID);
    }
    
    public int getPeerID() {
        return peerID;
    }
}