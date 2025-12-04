
PEERS=${@:-1002 1003 1004 1005 1006 1007}

javac -d bin peerProcess.java Handshake.java Message.java MessageType.java PeerInfo.java

pkill -9 -f peerProcess 2>/dev/null
sleep 1

echo "Starting peers: $PEERS"
echo "Starting peer 1001..."
java -cp bin peerProcess 1001 > "peer_1001_output.log" 2>&1 &
sleep 2

for peer_id in $PEERS; do
    echo "Starting peer $peer_id..."
    java -cp bin peerProcess $peer_id > "peer_${peer_id}_output.log" 2>&1 &
    sleep 0.05
done

echo "All peers started. Check logs for progress."
