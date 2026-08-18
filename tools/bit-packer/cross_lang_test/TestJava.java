import java.io.*;
import java.nio.file.*;
import generated.*;

public class TestJava {
    static Vec3Gen.WorldState createTestData() {
        Vec3Gen.WorldState w = new Vec3Gen.WorldState();
        w.world_id = 42;
        w.seed = "cross_lang_test";

        Vec3Gen.Character hero = new Vec3Gen.Character();
        hero.name = "TestHero";
        hero.level = 99;
        hero.hp = 1000;
        hero.mp = 500;
        hero.is_alive = true;
        hero.position = new Vec3Gen.Vec3();
        hero.position.x = 10;
        hero.position.y = -20;
        hero.position.z = 30;
        hero.skills = new int[]{1, 2, 3, 100};

        Vec3Gen.Item sword = new Vec3Gen.Item();
        sword.id = 1;
        sword.name = "Excalibur";
        sword.value = 9999;
        sword.weight = 15;
        sword.rarity = "Legendary";
        hero.inventory = new Vec3Gen.Item[]{sword};

        Vec3Gen.Guild guild = new Vec3Gen.Guild();
        guild.name = "TestGuild";
        guild.description = "A test guild for cross-language";
        guild.members = new Vec3Gen.Character[]{hero};

        w.guilds = new Vec3Gen.Guild[]{guild};

        Vec3Gen.Item potion = new Vec3Gen.Item();
        potion.id = 2;
        potion.name = "HealthPotion";
        potion.value = 50;
        potion.weight = 1;
        potion.rarity = "Common";
        w.loot_table = new Vec3Gen.Item[]{potion};
        return w;
    }

    static void verify(Vec3Gen.WorldState d, String label) {
        assert d.world_id == 42 : label + " world_id";
        assert d.seed.equals("cross_lang_test") : label + " seed";
        assert d.guilds.length == 1 : label + " guilds";
        assert d.guilds[0].name.equals("TestGuild") : label + " guild name";
        assert d.guilds[0].members[0].name.equals("TestHero") : label + " hero name";
        assert d.guilds[0].members[0].level == 99 : label + " level";
        assert d.guilds[0].members[0].hp == 1000 : label + " hp";
        assert d.guilds[0].members[0].position.x == 10 : label + " x";
        assert d.guilds[0].members[0].position.y == -20 : label + " y";
        assert d.guilds[0].members[0].position.z == 30 : label + " z";
        assert d.guilds[0].members[0].skills.length == 4 : label + " skills";
        assert d.guilds[0].members[0].inventory[0].name.equals("Excalibur") : label + " sword";
        assert d.guilds[0].members[0].inventory[0].value == 9999 : label + " sword val";
        assert d.loot_table[0].name.equals("HealthPotion") : label + " potion";
        assert d.loot_table[0].rarity.equals("Common") : label + " rarity";
    }

    public static void main(String[] args) throws Exception {
        System.out.println("☕ Java");

        // 1. Roundtrip
        Vec3Gen.WorldState w = createTestData();
        byte[] encoded = w.encode();
        System.out.println("   Encoded: " + encoded.length + " bytes");

        Vec3Gen.WorldState decoded = Vec3Gen.WorldState.decode(encoded);
        verify(decoded, "Java roundtrip");
        System.out.println("   ✅ Roundtrip PASS");

        // 2. Write to file
        String outFile = "test_data_java.bin";
        Files.write(Paths.get(outFile), encoded);
        System.out.println("   📁 Written to " + outFile);

        // 3. Cross-language: decode Python's data
        Path pyFile = Paths.get("test_data.bin");
        if (Files.exists(pyFile)) {
            byte[] pyData = Files.readAllBytes(pyFile);
            Vec3Gen.WorldState pyDecoded = Vec3Gen.WorldState.decode(pyData);
            verify(pyDecoded, "Java←Python cross-lang");
            System.out.println("   ✅ Cross-language decode (Python→Java) PASS");
        } else {
            System.out.println("   ⚠️ No Python test_data.bin found");
        }
    }
}
