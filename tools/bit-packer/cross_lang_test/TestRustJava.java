import java.io.*;
import java.nio.file.*;
import generated.*;

public class TestRustJava {
    public static void main(String[] args) throws Exception {
        byte[] rustData = Files.readAllBytes(Paths.get("test_data_rust.bin"));
        Vec3Gen.WorldState decoded = Vec3Gen.WorldState.decode(rustData);
        assert decoded.world_id == 42 : "world_id";
        assert decoded.guilds[0].members[0].name.equals("TestHero") : "hero";
        assert decoded.guilds[0].members[0].inventory[0].name.equals("Excalibur") : "sword";
        assert decoded.loot_table[0].name.equals("HealthPotion") : "potion";
        System.out.println("   ✅ Rust→Java cross-language decode PASS");
    }
}
