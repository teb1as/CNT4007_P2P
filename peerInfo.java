class PeerInfo {
    int peerID;
    String hostName;
    int port;
    int hasFile;

    public PeerInfo(int peerID, String hostName, int port, int hasFile) {
        this.peerID = peerID;
        this.hostName = hostName;
        this.port = port;
        this.hasFile = hasFile;
    }

    public int getPeerID() {
        return peerID;
    }

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }

    public int getHasFile() {
        return hasFile;
    }
}