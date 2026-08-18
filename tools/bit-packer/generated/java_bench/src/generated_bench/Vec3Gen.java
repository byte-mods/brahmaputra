package generated_bench;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Vec3Gen {
    public static final String VERSION = "1.0.0";

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

    
    public static class Vec3 {
        public int x;
        public int y;
        public int z;
        

        public byte[] encode() {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(65536);
            buf.putString(VERSION);
            encodeTo(buf);
            return buf.array();
        }

        public void encodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.putInt32(this.x);
            
            
            
            buf.putInt32(this.y);
            
            
            
            buf.putInt32(this.z);
            
            
        }

        public static Vec3 decode(byte[] data) throws Exception {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(data);
            String version = buf.getString();
            if (!version.equals(VERSION)) {
                throw new Exception("Version Mismatch: Expected " + VERSION + ", got " + version);
            }
            return decodeFrom(buf);
        }

        public static Vec3 decodeFrom(ZeroCopyByteBuff buf) throws Exception {
            Vec3 obj = new Vec3();
            
            
            obj.x = buf.getInt32();
            
            
            
            obj.y = buf.getInt32();
            
            
            
            obj.z = buf.getInt32();
            
            
            return obj;
        }
    }
    
    public static class Item {
        public int id;
        public String name;
        public int value;
        public int weight;
        public String rarity;
        

        public byte[] encode() {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(65536);
            buf.putString(VERSION);
            encodeTo(buf);
            return buf.array();
        }

        public void encodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.putInt32(this.id);
            
            
            
            buf.putString(this.name);
            
            
            
            buf.putInt32(this.value);
            
            
            
            buf.putInt32(this.weight);
            
            
            
            buf.putString(this.rarity);
            
            
        }

        public static Item decode(byte[] data) throws Exception {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(data);
            String version = buf.getString();
            if (!version.equals(VERSION)) {
                throw new Exception("Version Mismatch: Expected " + VERSION + ", got " + version);
            }
            return decodeFrom(buf);
        }

        public static Item decodeFrom(ZeroCopyByteBuff buf) throws Exception {
            Item obj = new Item();
            
            
            obj.id = buf.getInt32();
            
            
            
            obj.name = buf.getString();
            
            
            
            obj.value = buf.getInt32();
            
            
            
            obj.weight = buf.getInt32();
            
            
            
            obj.rarity = buf.getString();
            
            
            return obj;
        }
    }
    
    public static class Character {
        public String name;
        public int level;
        public int hp;
        public int mp;
        public boolean is_alive;
        public Vec3 position;
        public int[] skills;
        public Item[] inventory;
        

        public byte[] encode() {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(65536);
            buf.putString(VERSION);
            encodeTo(buf);
            return buf.array();
        }

        public void encodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.putString(this.name);
            
            
            
            buf.putInt32(this.level);
            
            
            
            buf.putInt32(this.hp);
            
            
            
            buf.putInt32(this.mp);
            
            
            
            buf.putBool(this.is_alive);
            
            
            
            this.position.encodeTo(buf);
            
            
            
            buf.putInt32(this.skills.length);
            for(int item : this.skills) {
                buf.putInt32(item);
            }
            
            
            
            buf.putInt32(this.inventory.length);
            for(Item item : this.inventory) {
                item.encodeTo(buf);
            }
            
            
        }

        public static Character decode(byte[] data) throws Exception {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(data);
            String version = buf.getString();
            if (!version.equals(VERSION)) {
                throw new Exception("Version Mismatch: Expected " + VERSION + ", got " + version);
            }
            return decodeFrom(buf);
        }

        public static Character decodeFrom(ZeroCopyByteBuff buf) throws Exception {
            Character obj = new Character();
            
            
            obj.name = buf.getString();
            
            
            
            obj.level = buf.getInt32();
            
            
            
            obj.hp = buf.getInt32();
            
            
            
            obj.mp = buf.getInt32();
            
            
            
            obj.is_alive = buf.getBool();
            
            
            
            obj.position = Vec3.decodeFrom(buf);
            
            
            
            int skillsLen = buf.getInt32();
            obj.skills = new int[skillsLen];
            for(int i=0; i<skillsLen; i++) {
                obj.skills[i] = buf.getInt32();
            }
            
            
            
            int inventoryLen = buf.getInt32();
            obj.inventory = new Item[inventoryLen];
            for(int i=0; i<inventoryLen; i++) {
                obj.inventory[i] = Item.decodeFrom(buf);
            }
            
            
            return obj;
        }
    }
    
    public static class Guild {
        public String name;
        public String description;
        public Character[] members;
        

        public byte[] encode() {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(65536);
            buf.putString(VERSION);
            encodeTo(buf);
            return buf.array();
        }

        public void encodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.putString(this.name);
            
            
            
            buf.putString(this.description);
            
            
            
            buf.putInt32(this.members.length);
            for(Character item : this.members) {
                item.encodeTo(buf);
            }
            
            
        }

        public static Guild decode(byte[] data) throws Exception {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(data);
            String version = buf.getString();
            if (!version.equals(VERSION)) {
                throw new Exception("Version Mismatch: Expected " + VERSION + ", got " + version);
            }
            return decodeFrom(buf);
        }

        public static Guild decodeFrom(ZeroCopyByteBuff buf) throws Exception {
            Guild obj = new Guild();
            
            
            obj.name = buf.getString();
            
            
            
            obj.description = buf.getString();
            
            
            
            int membersLen = buf.getInt32();
            obj.members = new Character[membersLen];
            for(int i=0; i<membersLen; i++) {
                obj.members[i] = Character.decodeFrom(buf);
            }
            
            
            return obj;
        }
    }
    
    public static class WorldState {
        public int world_id;
        public String seed;
        public Guild[] guilds;
        public Item[] loot_table;
        

        public byte[] encode() {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(65536);
            buf.putString(VERSION);
            encodeTo(buf);
            return buf.array();
        }

        public void encodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.putInt32(this.world_id);
            
            
            
            buf.putString(this.seed);
            
            
            
            buf.putInt32(this.guilds.length);
            for(Guild item : this.guilds) {
                item.encodeTo(buf);
            }
            
            
            
            buf.putInt32(this.loot_table.length);
            for(Item item : this.loot_table) {
                item.encodeTo(buf);
            }
            
            
        }

        public static WorldState decode(byte[] data) throws Exception {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(data);
            String version = buf.getString();
            if (!version.equals(VERSION)) {
                throw new Exception("Version Mismatch: Expected " + VERSION + ", got " + version);
            }
            return decodeFrom(buf);
        }

        public static WorldState decodeFrom(ZeroCopyByteBuff buf) throws Exception {
            WorldState obj = new WorldState();
            
            
            obj.world_id = buf.getInt32();
            
            
            
            obj.seed = buf.getString();
            
            
            
            int guildsLen = buf.getInt32();
            obj.guilds = new Guild[guildsLen];
            for(int i=0; i<guildsLen; i++) {
                obj.guilds[i] = Guild.decodeFrom(buf);
            }
            
            
            
            int loot_tableLen = buf.getInt32();
            obj.loot_table = new Item[loot_tableLen];
            for(int i=0; i<loot_tableLen; i++) {
                obj.loot_table[i] = Item.decodeFrom(buf);
            }
            
            
            return obj;
        }
    }
    
}
