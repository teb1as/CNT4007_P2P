import java.nio.ByteBuffer;
import java.io.DataInputStream;
import java.io.IOException;

enum MessageType {
    CHOKE,
    UNCHOKE,
    INTERESTED,
    NOT_INTERESTED,
    HAVE,
    BITFIELD,
    REQUEST,
    PIECE,
}