import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.*;

class CellTest {
    @Test
    void testReadVarInt(){
        byte b = 23;
        var buffer = ByteBuffer.allocate(1).order(ByteOrder.BIG_ENDIAN).put(b);
        buffer.position(0);
        VarInt l = Cell.from(buffer);
        Assertions.assertEquals(23L, l.value());
    }

    @Test
    void testReadVarInt2Bytes(){
        byte firstByte = (byte)0x87;
        byte secondByte = (byte)0x68;
        var buffer = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
        buffer.put(firstByte);
        buffer.put(secondByte);
        buffer.position(0);
        VarInt l = Cell.from(buffer);
        Assertions.assertEquals(1000, l.value());
    }

}