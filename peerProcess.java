import java.io.*;
import java.net.*;

public class peerProcess {
    private int peerID;
    
    public static void main(String[] args) {
        // check if user gave a peer id
        if (args.length != 1) {
            System.err.println("give peer id like this: java peerProcess 1001");
            System.exit(1);
        }
        
        try {
            int peerID = Integer.parseInt(args[0]);
            peerProcess peer = new peerProcess(peerID);
            peer.start();
        } 
        catch (Exception error) {
            System.err.println("error message: " + error.getMessage());
            System.exit(1);
        }
    }
    
    public peerProcess(int peerID) {
        this.peerID = peerID;
    }
    
    public void start() {
        System.out.println("peer " + peerID + " started");
        
        System.out.println("minimal implementation for testing");
        System.out.println("peer ID: " + peerID);
        
        String fileName = "thefile";
        int fileSize = 2167705;
        int pieceSize = 16384;
        
        System.out.println("file name is " + fileName);
        System.out.println("file size is " + fileSize + " bytes");
        System.out.println("piece size is " + pieceSize + " bytes");
        
        int numPieces = (fileSize + pieceSize - 1) / pieceSize;
        System.out.println("number of pieces: " + numPieces);
        
        Message chokeMsg = Message.choke();
        System.out.println("created choke message");
        
        try {
            Thread.sleep(2000);
        } 
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        System.out.println("peer " + peerID + " finished");
    }
}