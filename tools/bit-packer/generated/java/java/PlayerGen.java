package generated;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class PlayerGen {
    public static final String VERSION = "1.0.2";

    // --- ZeroCopyByteBuff ---
    public static class ZeroCopyByteBuff {
        public byte[] buf;
        public int offset;
        public int capacity;

        public ZeroCopyByteBuff(int capacity) {
            this.buf = new byte[capacity];
            this.capacity = capacity;
            this.offset = 0;
        }

        public ZeroCopyByteBuff(byte[] data) {
            this.buf = data;
            this.capacity = data.length;
            this.offset = 0;
        }

        public void ensureCapacity(int needed) {
            if (offset + needed > capacity) {
                int newCap = Math.max(capacity * 2, offset + needed);
                buf = Arrays.copyOf(buf, newCap);
                capacity = newCap;
            }
        }

        public byte[] array() {
            return Arrays.copyOf(buf, offset);
        }

        // Write Helpers
        public void putInt32(int v) {
            putVarInt64(v); // ZigZag encoded
        }

        public void putInt64(long v) {
            putVarInt64(v);
        }

        public void putVarInt64(long v) {
            // ZigZag encode: (n << 1) ^ (n >> 63)
            long zz = (v << 1) ^ (v >> 63);
            // FAST PATH: 1 byte (0-127, most common for game data)
            if ((zz & ~0x7FL) == 0) {
                if (offset >= capacity) ensureCapacity(1);
                buf[offset++] = (byte) zz;
                return;
            }
            // FAST PATH: 2 bytes (128-16383)
            if ((zz & ~0x3FFFL) == 0) {
                if (offset + 2 > capacity) ensureCapacity(2);
                buf[offset++] = (byte) ((zz & 0x7F) | 0x80);
                buf[offset++] = (byte) (zz >>> 7);
                return;
            }
            // General path
            ensureCapacity(10);
            while ((zz & ~0x7FL) != 0) {
                buf[offset++] = (byte) ((zz & 0x7F) | 0x80);
                zz >>>= 7;
            }
            buf[offset++] = (byte) zz;
        }
        
        public void putFloat(float v) {
            putVarInt64((long)(v * 10000.0f));
        }
        
        public void putDouble(double v) {
            putVarInt64((long)(v * 10000.0));
        }

        public void putBool(boolean v) {
            ensureCapacity(1);
            buf[offset++] = (byte) (v ? 1 : 0);
        }

        public void putString(String v) {
            byte[] bytes = v.getBytes(StandardCharsets.UTF_8);
            putVarInt64(bytes.length);
            ensureCapacity(bytes.length);
            System.arraycopy(bytes, 0, buf, offset, bytes.length);
            offset += bytes.length;
        }

        // Read Helpers
        public int getInt32() throws Exception {
            return (int) getVarInt64();
        }

        public long getInt64() throws Exception {
            return getVarInt64();
        }

        public long getVarInt64() throws Exception {
            long result = 0;
            int shift = 0;
            // FAST PATH: 1 byte
            byte b = buf[offset++];
            if ((b & 0x80) == 0) {
                result = b & 0x7F;
            } else {
                result = b & 0x7F;
                shift = 7;
                while (true) {
                    b = buf[offset++];
                    result |= (long) (b & 0x7F) << shift;
                    if ((b & 0x80) == 0) break;
                    shift += 7;
                }
            }
            // ZigZag decode: (n >>> 1) ^ -(n & 1)
            return (result >>> 1) ^ -(result & 1);
        }
        
        public float getFloat() throws Exception {
            return (float) getVarInt64() / 10000.0f;
        }
        
        public double getDouble() throws Exception {
            return (double) getVarInt64() / 10000.0;
        }

        public boolean getBool() throws Exception {
            if (offset >= capacity) throw new Exception("Buffer underflow");
            return buf[offset++] != 0;
        }

        public String getString() throws Exception {
            int len = (int) getVarInt64();
            if (offset + len > capacity) throw new Exception("Buffer underflow");
            String s = new String(buf, offset, len, StandardCharsets.UTF_8);
            offset += len;
            return s;
        }
    }

    
    public static class Player {
        public String username;
        public int level;
        public int score;
        public String[] inventory;
        

        public byte[] encode() {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(65536);
            buf.putString(VERSION);
            encodeTo(buf);
            return buf.array();
        }

        public void encodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.putString(this.username);
            
            
            
            buf.putInt32(this.level);
            
            
            
            buf.putInt32(this.score);
            
            
            
            buf.putInt32(this.inventory.length);
            for(String item : this.inventory) {
                buf.putString(item);
            }
            
            
        }

        public static Player decode(byte[] data) throws Exception {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(data);
            String version = buf.getString();
            if (!version.equals(VERSION)) {
                throw new Exception("Version Mismatch: Expected " + VERSION + ", got " + version);
            }
            return decodeFrom(buf);
        }

        public static Player decodeFrom(ZeroCopyByteBuff buf) throws Exception {
            Player obj = new Player();
            
            
            obj.username = buf.getString();
            
            
            
            obj.level = buf.getInt32();
            
            
            
            obj.score = buf.getInt32();
            
            
            
            int inventoryLen = buf.getInt32();
            obj.inventory = new String[inventoryLen];
            for(int i=0; i<inventoryLen; i++) {
                obj.inventory[i] = buf.getString();
            }
            
            
            return obj;
        }
    }
    
    public static class GameState {
        public int id;
        public boolean isActive;
        public Player[] players;
        

        public byte[] encode() {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(65536);
            buf.putString(VERSION);
            encodeTo(buf);
            return buf.array();
        }

        public void encodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.putInt32(this.id);
            
            
            
            buf.putBool(this.isActive);
            
            
            
            buf.putInt32(this.players.length);
            for(Player item : this.players) {
                item.encodeTo(buf);
            }
            
            
        }

        public static GameState decode(byte[] data) throws Exception {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(data);
            String version = buf.getString();
            if (!version.equals(VERSION)) {
                throw new Exception("Version Mismatch: Expected " + VERSION + ", got " + version);
            }
            return decodeFrom(buf);
        }

        public static GameState decodeFrom(ZeroCopyByteBuff buf) throws Exception {
            GameState obj = new GameState();
            
            
            obj.id = buf.getInt32();
            
            
            
            obj.isActive = buf.getBool();
            
            
            
            int playersLen = buf.getInt32();
            obj.players = new Player[playersLen];
            for(int i=0; i<playersLen; i++) {
                obj.players[i] = Player.decodeFrom(buf);
            }
            
            
            return obj;
        }
    }
    
}
