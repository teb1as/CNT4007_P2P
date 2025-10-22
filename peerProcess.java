import java.io.*;
import java.net.*;
import java.util.*;

public class peerProcess {
    private int peerID;
    
    private int numberOfPreferredNeighbors;
    private int unchokingInterval;
    private int optimisticUnchokingInterval;
    private String fileName;
    private int fileSize;
    private int pieceSize;

    private String hostName;
    private int port;
    private int hasFile;
    private Map<Integer, PeerInfo> allPeers;
    private ServerSocket serverSocket;
    private Map<Integer, Socket> connections;
    
    // Bitfield tracking
    private int numPieces;
    private boolean[] bitfield;  
    private Map<Integer, boolean[]> peerBitfields;  
    
    private Map<Integer, Boolean> interestedInPeer;  
    private Map<Integer, Boolean> peerInterestedInUs;  
    
    private Map<Integer, DataOutputStream> outputStreams;
    private Map<Integer, DataInputStream> inputStreams;
    
    private Map<Integer, Boolean> isChokingPeer;
    private Map<Integer, Boolean> isPeerChokingUs;
    private Set<Integer> preferredNeighbors;
    private Integer optimisticallyUnchokedPeer;
    private Map<Integer, Long> downloadRates;
    
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
        this.allPeers = new HashMap<>();
        this.connections = new HashMap<>();
        this.peerBitfields = new HashMap<>();
        this.interestedInPeer = new HashMap<>();
        this.peerInterestedInUs = new HashMap<>();
        this.outputStreams = new HashMap<>();
        this.inputStreams = new HashMap<>();
        this.isChokingPeer = new HashMap<>();
        this.isPeerChokingUs = new HashMap<>();
        this.preferredNeighbors = new HashSet<>();
        this.optimisticallyUnchokedPeer = null;
        this.downloadRates = new HashMap<>();
        loadConfiguration();
        loadPeerInfo();
        initializeBitfield();
        createPeerDirectory();
    }
    
    public void start() {
        System.out.println("=== PEER " + peerID + " STARTED!! ===");
        
        System.out.println("minimal implementation for testing");
        System.out.println("peer ID: " + peerID);
        // System.out.println("host: " + hostName + ":" + port);
        // System.out.println("has file: " + (hasFile == 1 ? "yes" : "no"));
        
        // System.out.println("file name is " + fileName);
        // System.out.println("file size is " + fileSize + " bytes");
        // System.out.println("piece size is " + pieceSize + " bytes");
        
        int numPieces = (fileSize + pieceSize - 1) / pieceSize;
        // System.out.println("number of pieces: " + numPieces);
        
        // System.out.println("preferred neighbors: " + numberOfPreferredNeighbors);
        // System.out.println("unchoking interval: " + unchokingInterval + " seconds");
        // System.out.println("optimistic unchoking interval: " + optimisticUnchokingInterval + " seconds");
        
        // System.out.println("total peers in network: " + allPeers.size());
        
        startServer();
        connectToLowerPeers();
        
        Timer unchokingTimer = new Timer(true);
        unchokingTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                selectPreferredNeighbors();
            }
        }, unchokingInterval * 1000, unchokingInterval * 1000);
        
        Timer optimisticTimer = new Timer(true);
        optimisticTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                selectOptimisticallyUnchokedNeighbor();
            }
        }, optimisticUnchokingInterval * 1000, optimisticUnchokingInterval * 1000);
        
        try {
            Thread.sleep(20000);
        } 

        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        unchokingTimer.cancel();
        optimisticTimer.cancel();
        cleanup();
        System.out.println("============= PEER " + peerID + " FINISHED!! ==================");
        System.out.println();
    }
    
    private void loadConfiguration() {

        try (BufferedReader reader = new BufferedReader(new FileReader("project_config_file_small/project_config_file_small/Common.cfg"))) {
            
            String line;

            while ((line = reader.readLine()) != null) {

                String[] parts = line.split(" ");

                switch (parts[0]) {
                    case "NumberOfPreferredNeighbors":
                        numberOfPreferredNeighbors = Integer.parseInt(parts[1]);
                        break;
                    case "UnchokingInterval":
                        unchokingInterval = Integer.parseInt(parts[1]);
                        break;
                    case "OptimisticUnchokingInterval":
                        optimisticUnchokingInterval = Integer.parseInt(parts[1]);
                        break;
                    case "FileName":
                        fileName = parts[1];
                        break;
                    case "FileSize":
                        fileSize = Integer.parseInt(parts[1]);
                        break;
                    case "PieceSize":
                        pieceSize = Integer.parseInt(parts[1]);
                        break;
                }

            }

            // System.out.println("loaded configuration from Common.cfg");
        } 
        
        catch (IOException e) {

            System.err.println("error loading Common.cfg: " + e.getMessage());
            System.exit(1);
            
        }
    }

    private void loadPeerInfo() {

        try (BufferedReader reader = new BufferedReader(new FileReader("project_config_file_small/project_config_file_small/PeerInfo.cfg"))) {
            
            String line;

            while ((line = reader.readLine()) != null) {

                if (line.trim().startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }

                String[] parts = line.trim().split("\\s+");

                if (parts.length >= 4) {
                    
                    int id = Integer.parseInt(parts[0]);
                    String host = parts[1];
                    int port = Integer.parseInt(parts[2]);
                    int hasFile = Integer.parseInt(parts[3]);
                    
                    allPeers.put(id, new PeerInfo(id, host, port, hasFile));
                    
                    if (id == peerID) {
                        this.hostName = host;
                        this.port = port;
                        this.hasFile = hasFile;
                    }

                }
            }

            // System.out.println("loaded peer info from PeerInfo.cfg");
        } 

        catch (IOException e) {
            System.err.println("error loading PeerInfo.cfg: " + e.getMessage());
            System.exit(1);
        }
    }

    private void initializeBitfield() {
        numPieces = (fileSize + pieceSize - 1) / pieceSize;
        bitfield = new boolean[numPieces];
        
        // If this peer has the complete file, set all bits to true
        if (hasFile == 1) {
            for (int i = 0; i < numPieces; i++) {
                bitfield[i] = true;
            }
            System.out.println("initialized bitfield: peer has all " + numPieces + " pieces");
        } else {
            System.out.println("initialized bitfield: peer has 0/" + numPieces + " pieces");
        }
    }
    
    private byte[] createBitfieldBytes() {
        
        int numBytes = (numPieces + 7) / 8;
        byte[] bitfieldBytes = new byte[numBytes];
        
        for (int i = 0; i < numPieces; i++) {
            if (bitfield[i]) {
                int byteIndex = i / 8;
                int bitIndex = 7 - (i % 8);  
                bitfieldBytes[byteIndex] |= (1 << bitIndex);
            }
        }
        
        return bitfieldBytes;
    }
    
    private void processBitfield(int peerID, byte[] bitfieldBytes) {
        boolean[] peerBitfield = new boolean[numPieces];
        
        for (int i = 0; i < numPieces; i++) {
            int byteIndex = i / 8;
            int bitIndex = 7 - (i % 8);  // MSB first
            if (byteIndex < bitfieldBytes.length) {
                peerBitfield[i] = ((bitfieldBytes[byteIndex] >> bitIndex) & 1) == 1;
            }
        }
        
        peerBitfields.put(peerID, peerBitfield);
        
       
        int piecesCount = 0;
        for (boolean hasPiece : peerBitfield) {
            if (hasPiece) piecesCount++;
        }
        
        System.out.println("peer " + peerID + " has " + piecesCount + "/" + numPieces + " pieces");
    }
    
    private boolean isInterestedInPeer(int remotePeerID) {
        if (!peerBitfields.containsKey(remotePeerID)) {
            return false;
        }
        
        boolean[] remoteBitfield = peerBitfields.get(remotePeerID);
        for (int i = 0; i < numPieces; i++) {
            if (remoteBitfield[i] && !bitfield[i]) {
                return true;
            }
        }
        return false;
    }
    
    private void sendInterestedMessage(int remotePeerID) throws IOException {
        if (!outputStreams.containsKey(remotePeerID)) {
            return;
        }
        
        Message msg = Message.interested();
        outputStreams.get(remotePeerID).write(msg.toBytes());
        outputStreams.get(remotePeerID).flush();
        interestedInPeer.put(remotePeerID, true);
        System.out.println("sent INTERESTED to peer " + remotePeerID);
    }
    
    private void sendNotInterestedMessage(int remotePeerID) throws IOException {
        if (!outputStreams.containsKey(remotePeerID)) {
            return;
        }
        
        Message msg = Message.notInterested();
        outputStreams.get(remotePeerID).write(msg.toBytes());
        outputStreams.get(remotePeerID).flush();
        interestedInPeer.put(remotePeerID, false);
        System.out.println("sent NOT_INTERESTED to peer " + remotePeerID);
    }
    
    private void determineInterestAndNotify(int remotePeerID) {
        try {
            if (isInterestedInPeer(remotePeerID)) {
                sendInterestedMessage(remotePeerID);
            } else {
                sendNotInterestedMessage(remotePeerID);
            }
        } catch (IOException e) {
            System.err.println("error sending interest message to peer " + remotePeerID + ": " + e.getMessage());
        }
    }
    
    private void sendChokeMessage(int remotePeerID) throws IOException {
        if (!outputStreams.containsKey(remotePeerID)) {
            return;
        }
        
        Message msg = Message.choke();
        outputStreams.get(remotePeerID).write(msg.toBytes());
        outputStreams.get(remotePeerID).flush();
        isChokingPeer.put(remotePeerID, true);
        System.out.println("sent CHOKE to peer " + remotePeerID);
    }
    
    private void sendUnchokeMessage(int remotePeerID) throws IOException {
        if (!outputStreams.containsKey(remotePeerID)) {
            return;
        }
        
        Message msg = Message.unchoke();
        outputStreams.get(remotePeerID).write(msg.toBytes());
        outputStreams.get(remotePeerID).flush();
        isChokingPeer.put(remotePeerID, false);
        System.out.println("sent UNCHOKE to peer " + remotePeerID);
    }
    
    private void selectPreferredNeighbors() {
        Set<Integer> newPreferredNeighbors = new HashSet<>();
        List<Integer> interestedPeers = new ArrayList<>();
        
        for (Integer peerID : peerInterestedInUs.keySet()) {
            if (peerInterestedInUs.get(peerID)) {
                interestedPeers.add(peerID);
            }
        }
        
        if (hasFile == 1) {
            Collections.shuffle(interestedPeers);
        } else {
            interestedPeers.sort((p1, p2) -> {
                long rate1 = downloadRates.getOrDefault(p1, 0L);
                long rate2 = downloadRates.getOrDefault(p2, 0L);
                return Long.compare(rate2, rate1);
            });
        }
        
        int numToSelect = Math.min(numberOfPreferredNeighbors, interestedPeers.size());
        for (int i = 0; i < numToSelect; i++) {
            newPreferredNeighbors.add(interestedPeers.get(i));
        }
        
        for (Integer peerID : connections.keySet()) {
            try {
                boolean wasPreferred = preferredNeighbors.contains(peerID);
                boolean isPreferred = newPreferredNeighbors.contains(peerID);
                boolean isOptimistic = (optimisticallyUnchokedPeer != null && optimisticallyUnchokedPeer.equals(peerID));
                
                if (isPreferred || isOptimistic) {
                    if (isChokingPeer.getOrDefault(peerID, true)) {
                        sendUnchokeMessage(peerID);
                    }
                } else {
                    if (!isChokingPeer.getOrDefault(peerID, true)) {
                        sendChokeMessage(peerID);
                    }
                }
            } catch (IOException e) {
                System.err.println("error updating choke state for peer " + peerID + ": " + e.getMessage());
            }
        }
        
        preferredNeighbors = newPreferredNeighbors;
        
        if (!preferredNeighbors.isEmpty()) {
            System.out.println("preferred neighbors: " + preferredNeighbors);
        }
    }
    
    private void selectOptimisticallyUnchokedNeighbor() {
        List<Integer> chokedInterestedPeers = new ArrayList<>();
        
        for (Integer peerID : peerInterestedInUs.keySet()) {
            if (peerInterestedInUs.get(peerID) && 
                !preferredNeighbors.contains(peerID) &&
                isChokingPeer.getOrDefault(peerID, true)) {
                chokedInterestedPeers.add(peerID);
            }
        }
        
        if (!chokedInterestedPeers.isEmpty()) {
            int randomIndex = new Random().nextInt(chokedInterestedPeers.size());
            Integer newOptimistic = chokedInterestedPeers.get(randomIndex);
            
            if (optimisticallyUnchokedPeer != null && !optimisticallyUnchokedPeer.equals(newOptimistic)) {
                try {
                    if (!preferredNeighbors.contains(optimisticallyUnchokedPeer)) {
                        sendChokeMessage(optimisticallyUnchokedPeer);
                    }
                } catch (IOException e) {
                    System.err.println("error choking previous optimistic peer: " + e.getMessage());
                }
            }
            
            optimisticallyUnchokedPeer = newOptimistic;
            try {
                sendUnchokeMessage(optimisticallyUnchokedPeer);
                System.out.println("optimistically unchoked peer " + optimisticallyUnchokedPeer);
            } catch (IOException e) {
                System.err.println("error unchoking optimistic peer: " + e.getMessage());
            }
        }
    }
    
    private void createPeerDirectory() {
        
        try {
            
            File peerDir = new File("peer_" + peerID);
            
            if (!peerDir.exists()) {
                peerDir.mkdir();
                // System.out.println("created directory: peer_" + peerID);
            } 

            else {
                // System.out.println("directory already exists: peer_" + peerID);
            }

        } 
        
        catch (Exception e) {
            System.err.println("error creating peer directory: " + e.getMessage());
        }

    }
    
    private void startServer() {
        
        try {
           
            serverSocket = new ServerSocket(port);
            System.out.println("server listening on port " + port);
            
            Thread serverThread = new Thread(() -> {
                try {
                    
                    while (true) {
                        Socket clientSocket = serverSocket.accept();
                        System.out.println("accepted connection from " + clientSocket.getInetAddress());
                        handleIncomingConnection(clientSocket);
                    }

                } 
                
                catch (IOException e) {
                    if (!serverSocket.isClosed()) {
                        System.err.println("server error: " + e.getMessage());
                    }
                }

            });

            serverThread.setDaemon(true);
            serverThread.start();
            
        } 
        
        catch (IOException e) {
            System.err.println("error starting server: " + e.getMessage());
        }

    }
    
    private void connectToLowerPeers() {

        for (PeerInfo peer : allPeers.values()) {
            if (peer.getPeerID() < peerID) {
                connectToPeer(peer);
            }
        }

    }
    
    private void connectToPeer(PeerInfo peer) {
        
        try {
            Socket socket = new Socket(peer.getHostName(), peer.getPort());
            System.out.println("connected to peer " + peer.getPeerID());
            
            // Send handshake
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            Handshake handshake = new Handshake(peerID);
            out.write(handshake.toBytes());
            out.flush();
            System.out.println("sent handshake to peer " + peer.getPeerID());
            
            // Receive handshake response
            DataInputStream in = new DataInputStream(socket.getInputStream());
            Handshake receivedHandshake = Handshake.read(in);
            
            if (receivedHandshake.getPeerID() == peer.getPeerID()) {
                System.out.println("handshake validated for peer " + peer.getPeerID());
                connections.put(peer.getPeerID(), socket);
                outputStreams.put(peer.getPeerID(), out);
                inputStreams.put(peer.getPeerID(), in);
                isChokingPeer.put(peer.getPeerID(), true);
                isPeerChokingUs.put(peer.getPeerID(), true);
                
                // Exchange bitfield messages
                // Send our bitfield
                byte[] bitfieldBytes = createBitfieldBytes();
                Message bitfieldMsg = Message.bitfield(bitfieldBytes);
                out.write(bitfieldMsg.toBytes());
                out.flush();
                System.out.println("sent bitfield to peer " + peer.getPeerID());
                
                // Receive their bitfield
                Message receivedMsg = Message.read(in);
                if (receivedMsg.getType() == MessageType.BITFIELD) {
                    processBitfield(peer.getPeerID(), receivedMsg.getPayload());
                    System.out.println("received bitfield from peer " + peer.getPeerID());
                }
                
                determineInterestAndNotify(peer.getPeerID());
                
                Message interestMsg = Message.read(in);
                if (interestMsg.getType() == MessageType.INTERESTED) {
                    peerInterestedInUs.put(peer.getPeerID(), true);
                    System.out.println("peer " + peer.getPeerID() + " is INTERESTED");
                } else if (interestMsg.getType() == MessageType.NOT_INTERESTED) {
                    peerInterestedInUs.put(peer.getPeerID(), false);
                    System.out.println("peer " + peer.getPeerID() + " is NOT_INTERESTED");
                }
            } else {
                System.err.println("handshake failed: expected peer " + peer.getPeerID() + 
                                 ", got peer " + receivedHandshake.getPeerID());
                socket.close();
            }
        } 
        
        catch (IOException e) {
            System.err.println("error connecting to peer " + peer.getPeerID() + ": " + e.getMessage());
        }

    }
    
    private void handleIncomingConnection(Socket socket) {
        try {
            System.out.println("handling incoming connection from " + socket.getInetAddress());
            
            DataInputStream in = new DataInputStream(socket.getInputStream());
            Handshake receivedHandshake = Handshake.read(in);
            int remotePeerID = receivedHandshake.getPeerID();

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            Handshake handshake = new Handshake(peerID);
            out.write(handshake.toBytes());
            out.flush();
            System.out.println("sent handshake to peer " + remotePeerID);
            
            if (allPeers.containsKey(remotePeerID)) {
                System.out.println("handshake completed with peer " + remotePeerID);
                connections.put(remotePeerID, socket);
                outputStreams.put(remotePeerID, out);
                inputStreams.put(remotePeerID, in);
                isChokingPeer.put(remotePeerID, true);
                isPeerChokingUs.put(remotePeerID, true);
                
                byte[] bitfieldBytes = createBitfieldBytes();
                Message bitfieldMsg = Message.bitfield(bitfieldBytes);
                out.write(bitfieldMsg.toBytes());
                out.flush();
                System.out.println("sent bitfield to peer " + remotePeerID);
                
                Message receivedMsg = Message.read(in);
                if (receivedMsg.getType() == MessageType.BITFIELD) {
                    processBitfield(remotePeerID, receivedMsg.getPayload());
                    System.out.println("received bitfield from peer " + remotePeerID);
                }
                
                determineInterestAndNotify(remotePeerID);
                
                Message interestMsg = Message.read(in);
                if (interestMsg.getType() == MessageType.INTERESTED) {
                    peerInterestedInUs.put(remotePeerID, true);
                    System.out.println("peer " + remotePeerID + " is INTERESTED");
                } else if (interestMsg.getType() == MessageType.NOT_INTERESTED) {
                    peerInterestedInUs.put(remotePeerID, false);
                    System.out.println("peer " + remotePeerID + " is NOT_INTERESTED");
                }
            } else {
                System.err.println("unknown peer " + remotePeerID + " tried to connect");
                socket.close();
            }
        } 
        
        catch (Exception e) {

            System.err.println("error handling connection: " + e.getMessage());
            
            try {
                socket.close();
            } 
            catch (IOException ex) {

            }
        }

    }
    
    private void cleanup() {

        System.out.println("cleaning up connections");

        try {
            if (serverSocket != null) {
                serverSocket.close();
            }

            for (Socket socket : connections.values()) {
                socket.close();
            }

        } 
        
        catch (IOException e) {
        }
    }
}