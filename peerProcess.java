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
    private Map<Integer, Long> bytesReceivedThisInterval;
    private PrintWriter logWriter;
    
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
        this.bytesReceivedThisInterval = new HashMap<>();
        loadConfiguration();
        loadPeerInfo();
        initializeBitfield();
        createPeerDirectory();
        initializeLogging();
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
            Thread.sleep(120000); 
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

        try (BufferedReader reader = new BufferedReader(new FileReader("project_config_file_large/Common.cfg"))) {
            
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

        try (BufferedReader reader = new BufferedReader(new FileReader("project_config_file_large/PeerInfo.cfg"))) {
            
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
            boolean currentlyInterested = isInterestedInPeer(remotePeerID);
            boolean wasInterested = interestedInPeer.getOrDefault(remotePeerID, false);
            
            if (currentlyInterested != wasInterested) {
                if (currentlyInterested) {
                    sendInterestedMessage(remotePeerID);
                } 
                
                else {
                    sendNotInterestedMessage(remotePeerID);
                }
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
        long intervalSeconds = unchokingInterval;
        
        for (Integer peerID : bytesReceivedThisInterval.keySet()) {
            long bytesReceived = bytesReceivedThisInterval.get(peerID);
            long rate = bytesReceived / intervalSeconds;
            downloadRates.put(peerID, rate);
        }
        
        bytesReceivedThisInterval.clear();
        
        Set<Integer> newPreferredNeighbors = new HashSet<>();
        List<Integer> interestedPeers = new ArrayList<>();
        
        for (Integer peerID : peerInterestedInUs.keySet()) {
            if (peerInterestedInUs.get(peerID)) {
                interestedPeers.add(peerID);
            }
        }
        
        if (hasFile == 1) {
            Collections.shuffle(interestedPeers);
        } 
        
        else {
            Map<Long, List<Integer>> rateGroups = new HashMap<>();
            for (Integer peerID : interestedPeers) {
                long rate = downloadRates.getOrDefault(peerID, 0L);
                rateGroups.computeIfAbsent(rate, k -> new ArrayList<>()).add(peerID);
            }
            
            List<Long> sortedRates = new ArrayList<>(rateGroups.keySet());
            Collections.sort(sortedRates, Collections.reverseOrder());
            
            interestedPeers.clear();
            Random random = new Random();
            for (Long rate : sortedRates) {
                List<Integer> peersAtRate = rateGroups.get(rate);
                Collections.shuffle(peersAtRate, random);
                interestedPeers.addAll(peersAtRate);
            }
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
        
        Set<Integer> oldPreferredNeighbors = new HashSet<>(preferredNeighbors);
        preferredNeighbors = newPreferredNeighbors;
        
        if (!preferredNeighbors.equals(oldPreferredNeighbors) && !preferredNeighbors.isEmpty()) {
            System.out.println("preferred neighbors: " + preferredNeighbors);
            
            List<Integer> sortedList = new ArrayList<>(preferredNeighbors);
            Collections.sort(sortedList);
            StringBuilder neighborList = new StringBuilder();
            for (int i = 0; i < sortedList.size(); i++) {
                if (i > 0) neighborList.append(",");
                neighborList.append(sortedList.get(i));
            }
            
            log("Peer " + peerID + " has the preferred neighbors " + neighborList.toString() + ".");
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
            
            if (optimisticallyUnchokedPeer == null || !newOptimistic.equals(optimisticallyUnchokedPeer)) {
                if (optimisticallyUnchokedPeer != null) {
                    try {
                        if (!preferredNeighbors.contains(optimisticallyUnchokedPeer)) {
                            sendChokeMessage(optimisticallyUnchokedPeer);
                        }
                    } 
                    
                    catch (IOException e) {
                        System.err.println("error choking previous optimistic peer: " + e.getMessage());
                    }
                }
                
                optimisticallyUnchokedPeer = newOptimistic;
                try {
                    sendUnchokeMessage(optimisticallyUnchokedPeer);
                    System.out.println("optimistically unchoked peer " + optimisticallyUnchokedPeer);
                    log("Peer " + peerID + " has the optimistically unchoked neighbor " + optimisticallyUnchokedPeer + ".");
                } 
                
                catch (IOException e) {
                    System.err.println("error unchoking optimistic peer: " + e.getMessage());
                }
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
    
    private void initializeLogging() {
        try {
            File logFile = new File("log_peer_" + peerID + ".log");
            logWriter = new PrintWriter(new FileWriter(logFile));
        } 
        catch (IOException e) {
            System.err.println("error creating log file: " + e.getMessage());
        }
    }
    
    private String getTimestamp() {
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new java.util.Date());
    }
    
    private void log(String message) {
        if (logWriter != null) {
            logWriter.println("[" + getTimestamp() + "]: " + message);
            logWriter.flush();
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
            log("Peer " + peerID + " makes a connection to Peer " + peer.getPeerID() + ".");
            
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
                
                byte[] bitfieldBytes = createBitfieldBytes();
                Message bitfieldMsg = Message.bitfield(bitfieldBytes);
                out.write(bitfieldMsg.toBytes());
                out.flush();
                System.out.println("sent bitfield to peer " + peer.getPeerID());
                
                Message receivedMsg = Message.read(in);
                if (receivedMsg.getType() == MessageType.BITFIELD) {
                    processBitfield(peer.getPeerID(), receivedMsg.getPayload());
                    System.out.println("received bitfield from peer " + peer.getPeerID());
                }
                
                determineInterestAndNotify(peer.getPeerID());
                startMessageHandler(peer.getPeerID());
            } 
            
            else {
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
                log("Peer " + peerID + " is connected from Peer " + remotePeerID + ".");
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
                startMessageHandler(remotePeerID);
            } 
            
            else {
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
    
    private void startMessageHandler(int peerID) {
        Thread messageThread = new Thread(() -> {
            handleMessages(peerID);
        });
        messageThread.setDaemon(true);
        messageThread.start();
    }
    
    private void handleMessages(int peerID) {
        boolean testing = true;
        DataInputStream in = inputStreams.get(peerID);
        if (in == null) {
            System.out.println("ERROR: input stream is null for peer " + peerID);
            return;
        }
        
        System.out.println("Message handler started for peer " + peerID + ", waiting for messages...");
        int counter = 0;
        try {
            while (true) {
                Message msg = Message.read(in);
                if (msg != null) {
                    if (msg.getType() == MessageType.REQUEST) {
                        counter += 1;
                    }
                    if (msg.getType() != MessageType.REQUEST && msg.getType() != MessageType.HAVE){
                        System.out.println("[" + this.peerID + "] received message type " + msg.getType() + " from peer " + peerID);
                    }
                    //System.out.println("[" + this.peerID + "] received message type " + msg.getType() + " from peer " + peerID);
                    processMessage(peerID, msg);
                }
            }
        } 
        catch (java.io.EOFException e) {
            System.out.println("sent " + counter + " PIECES to peer " + peerID);
            System.out.println("[" + this.peerID + "] received message type REQUEST " + counter + " times from peer " + peerID);
            System.out.println("[" + this.peerID + "] received message type HAVE " + (counter - 1) + " times from peer " + peerID);
            System.out.println("connection closed by peer " + peerID);
        } 
        catch (java.net.SocketException e) {
            System.out.println("sent " + counter + " PIECES to peer " + peerID);
            System.out.println("[" + this.peerID + "] received message type REQUEST " + counter + " times from peer " + peerID);
            System.out.println("[" + this.peerID + "] received message type HAVE " + (counter - 1) + " times from peer " + peerID);
            System.out.println("socket closed for peer " + peerID);
        } 
        catch (IOException e) {
            String msg = e.getMessage();
            if (msg == null) msg = e.getClass().getSimpleName();
            if (!msg.contains("Socket closed") && !msg.contains("Connection reset")) {
                System.err.println("error reading from peer " + peerID + ": " + msg);
            }
        } 
        catch (Exception e) {
            String msg = e.getMessage();
            if (msg == null) msg = e.getClass().getSimpleName();
            System.err.println("error processing message from peer " + peerID + ": " + msg);
            e.printStackTrace();
        }
    }
    
    private void processMessage(int peerID, Message msg) {
        switch (msg.getType()) {
            case CHOKE:
                isPeerChokingUs.put(peerID, true);
                System.out.println("peer " + peerID + " CHOKED us");
                log("Peer " + this.peerID + " is choked by " + peerID + ".");
                break;
                
            case UNCHOKE:
                isPeerChokingUs.put(peerID, false);
                System.out.println("[" + this.peerID + "] RECEIVED UNCHOKE from peer " + peerID);
                log("Peer " + this.peerID + " is unchoked by " + peerID + ".");
                requestPieceIfNeeded(peerID);
                break;
                
            case INTERESTED:
                peerInterestedInUs.put(peerID, true);
                System.out.println("peer " + peerID + " is INTERESTED");
                log("Peer " + this.peerID + " received the 'interested' message from " + peerID + ".");
                
                if (hasFile == 1 && preferredNeighbors.size() < numberOfPreferredNeighbors) {
                    if (isChokingPeer.getOrDefault(peerID, true)) {
                        preferredNeighbors.add(peerID);
                        
                        try {
                            sendUnchokeMessage(peerID);
                            System.out.println("immediately added peer " + peerID + " as preferred neighbor");
                            
                            List<Integer> sortedList = new ArrayList<>(preferredNeighbors);
                            Collections.sort(sortedList);
                            StringBuilder neighborList = new StringBuilder();
                            for (int i = 0; i < sortedList.size(); i++) {
                                if (i > 0) neighborList.append(",");
                                neighborList.append(sortedList.get(i));
                            }

                            log("Peer " + this.peerID + " has the preferred neighbors " + neighborList.toString() + ".");
                        } 
                        
                        catch (IOException e) {
                            System.err.println("error sending immediate UNCHOKE: " + e.getMessage());
                        }
                    }
                }
                break;
                
            case NOT_INTERESTED:
                peerInterestedInUs.put(peerID, false);
                System.out.println("peer " + peerID + " is NOT_INTERESTED");
                log("Peer " + this.peerID + " received the 'not interested' message from " + peerID + ".");
                break;
                
            case HAVE:
                if (msg.getPayload() != null && msg.getPayload().length >= 4) {
                    int pieceIndex = java.nio.ByteBuffer.wrap(msg.getPayload()).getInt();
                    updatePeerBitfield(peerID, pieceIndex);

                    // Only print/log HAVE when it's relevant (i.e., we don't already have that piece)
                    if (pieceIndex >= 0 && pieceIndex < numPieces && !bitfield[pieceIndex]) {
                        System.out.println("peer " + peerID + " has piece " + pieceIndex);
                        log("Peer " + this.peerID + " received the 'have' message from " + peerID + " for the piece " + pieceIndex + ".");
                    }

                    determineInterestAndNotify(peerID);
                }
                break;
                
            case REQUEST:
                if (!isChokingPeer.getOrDefault(peerID, true)) {
                    if (msg.getPayload() != null && msg.getPayload().length >= 4) {
                        int requestedPiece = java.nio.ByteBuffer.wrap(msg.getPayload()).getInt();
                        sendPiece(peerID, requestedPiece);
                    }
                }
                break;
                
            case PIECE:
                if (msg.getPayload() != null && msg.getPayload().length >= 4) {
                    int receivedPiece = java.nio.ByteBuffer.wrap(msg.getPayload(), 0, 4).getInt();
                    byte[] pieceData = new byte[msg.getPayload().length - 4];
                    System.arraycopy(msg.getPayload(), 4, pieceData, 0, pieceData.length);
                    handleReceivedPiece(peerID, receivedPiece, pieceData);
                }
                break;
                
            default:
                System.out.println("received unknown message type from peer " + peerID);
        }
    }
    
    private void requestPieceIfNeeded(int peerID) {
        if (isPeerChokingUs.getOrDefault(peerID, true)) {
            return;
        }
        
        boolean[] peerBitfield = peerBitfields.get(peerID);
        if (peerBitfield == null) {
            return;
        }
        
        List<Integer> neededPieces = new ArrayList<>();
        for (int i = 0; i < numPieces; i++) {
            if (!bitfield[i] && peerBitfield[i]) {
                neededPieces.add(i);
            }
        }
        
        if (!neededPieces.isEmpty()) {
            int randomPiece = neededPieces.get(new Random().nextInt(neededPieces.size()));
            sendRequest(peerID, randomPiece);
        }
    }
    
    private void sendRequest(int peerID, int pieceIndex) {
        try {
            DataOutputStream out = outputStreams.get(peerID);
            if (out != null) {
                Message requestMsg = Message.request(pieceIndex);
                out.write(requestMsg.toBytes());
                out.flush();
                System.out.println("sent REQUEST for piece " + pieceIndex + " to peer " + peerID);
            }
        } catch (IOException e) {
            System.err.println("error sending REQUEST to peer " + peerID + ": " + e.getMessage());
        }
    }
    
    private void sendPiece(int peerID, int pieceIndex) {
        try {
            if (bitfield[pieceIndex]) {
                byte[] pieceData = readPieceFromFile(pieceIndex);
                DataOutputStream out = outputStreams.get(peerID);
                if (out != null) {
                    Message pieceMsg = Message.piece(pieceIndex, pieceData);
                    out.write(pieceMsg.toBytes());
                    out.flush();
                    //System.out.println("sent PIECE " + pieceIndex + " to peer " + peerID);
                }
            }
        } catch (IOException e) {
            System.err.println("error sending PIECE to peer " + peerID + ": " + e.getMessage());
        }
    }
    
    private void handleReceivedPiece(int peerID, int pieceIndex, byte[] pieceData) {
        try {
            writePieceToFile(pieceIndex, pieceData);
            bitfield[pieceIndex] = true;
            
            long bytesReceived = bytesReceivedThisInterval.getOrDefault(peerID, 0L);
            bytesReceivedThisInterval.put(peerID, bytesReceived + pieceData.length);
            
            int piecesCount = 0;
            for (boolean hasPiece : bitfield) {
                if (hasPiece) piecesCount++;
            }
            
            System.out.println("downloaded piece " + pieceIndex + " from peer " + peerID);
            log("Peer " + this.peerID + " has downloaded the piece " + pieceIndex + " from " + peerID + ". Now the number of pieces it has is " + piecesCount + ".");
            
            if (piecesCount == numPieces) {
                log("Peer " + this.peerID + " has downloaded the complete file.");
                System.out.println("Peer " + this.peerID + " has downloaded the complete file. Cleaning up and exiting.");
                cleanup();
                System.exit(0);
            }
            
            broadcastHave(pieceIndex);
            determineInterestAndNotify(peerID);
            
            if (!isPeerChokingUs.getOrDefault(peerID, true)) {
                requestPieceIfNeeded(peerID);
            }
        } catch (IOException e) {
            System.err.println("error handling received piece: " + e.getMessage());
        }
    }
    
    private void updatePeerBitfield(int peerID, int pieceIndex) {
        boolean[] peerBitfield = peerBitfields.get(peerID);
        if (peerBitfield != null && pieceIndex < peerBitfield.length) {
            peerBitfield[pieceIndex] = true;
        }
    }
    
    private void broadcastHave(int pieceIndex) {
        Message haveMsg = Message.have(pieceIndex);
        for (Map.Entry<Integer, DataOutputStream> entry : outputStreams.entrySet()) {
            try {
                entry.getValue().write(haveMsg.toBytes());
                entry.getValue().flush();
            } catch (IOException e) {
                System.err.println("error broadcasting HAVE to peer " + entry.getKey() + ": " + e.getMessage());
            }
        }
    }
    
    private byte[] readPieceFromFile(int pieceIndex) throws IOException {
        File file = new File("peer_" + peerID + "/" + fileName);
        try (FileInputStream fis = new FileInputStream(file)) {
            int offset = pieceIndex * pieceSize;
            fis.skip(offset);
            
            int bytesToRead = pieceSize;
            if (pieceIndex == numPieces - 1) {
                int lastPieceSize = fileSize - (numPieces - 1) * pieceSize;
                bytesToRead = lastPieceSize;
            }
            
            byte[] data = new byte[bytesToRead];
            fis.read(data);
            return data;
        }
    }
    
    private void writePieceToFile(int pieceIndex, byte[] data) throws IOException {
        File file = new File("peer_" + peerID + "/" + fileName);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
            file.createNewFile();
        }
        
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            int offset = pieceIndex * pieceSize;
            raf.seek(offset);
            raf.write(data);
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
            
            if (logWriter != null) {
                logWriter.close();
            }

        } 
        
        catch (IOException e) {
        }
    }
}
