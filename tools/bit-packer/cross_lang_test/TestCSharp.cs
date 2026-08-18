// C# roundtrip test + cross-language decode test
// Compile: csc -out:TestCSharp.exe TestCSharp.cs generated/csharp/bench_complex.cs
// Run: mono TestCSharp.exe  (or dotnet-script, or just ./TestCSharp.exe on .NET)

using System;
using System.IO;
using Generated;

class TestCSharp {
    static WorldState CreateTestData() {
        var w = new WorldState();
        w.world_id = 42;
        w.seed = "cross_lang_test";

        var hero = new Character();
        hero.name = "TestHero";
        hero.level = 99;
        hero.hp = 1000;
        hero.mp = 500;
        hero.is_alive = true;
        hero.position = new Vec3();
        hero.position.x = 10;
        hero.position.y = -20;
        hero.position.z = 30;
        hero.skills = new int[] { 1, 2, 3, 100 };

        var sword = new Item();
        sword.id = 1;
        sword.name = "Excalibur";
        sword.value = 9999;
        sword.weight = 15;
        sword.rarity = "Legendary";
        hero.inventory = new Item[] { sword };

        var guild = new Guild();
        guild.name = "TestGuild";
        guild.description = "A test guild for cross-language";
        guild.members = new Character[] { hero };

        w.guilds = new Guild[] { guild };

        var potion = new Item();
        potion.id = 2;
        potion.name = "HealthPotion";
        potion.value = 50;
        potion.weight = 1;
        potion.rarity = "Common";
        w.loot_table = new Item[] { potion };
        return w;
    }

    static void Verify(WorldState d, string label) {
        if (d.world_id != 42) throw new Exception($"{label}: world_id={d.world_id}");
        if (d.seed != "cross_lang_test") throw new Exception($"{label}: seed={d.seed}");
        if (d.guilds.Length != 1) throw new Exception($"{label}: guilds={d.guilds.Length}");
        if (d.guilds[0].name != "TestGuild") throw new Exception($"{label}: guild={d.guilds[0].name}");
        if (d.guilds[0].description != "A test guild for cross-language") throw new Exception($"{label}: desc");
        var h = d.guilds[0].members[0];
        if (h.name != "TestHero") throw new Exception($"{label}: hero={h.name}");
        if (h.level != 99) throw new Exception($"{label}: level={h.level}");
        if (h.hp != 1000) throw new Exception($"{label}: hp={h.hp}");
        if (h.mp != 500) throw new Exception($"{label}: mp={h.mp}");
        if (!h.is_alive) throw new Exception($"{label}: alive");
        if (h.position.x != 10) throw new Exception($"{label}: x={h.position.x}");
        if (h.position.y != -20) throw new Exception($"{label}: y={h.position.y}");
        if (h.position.z != 30) throw new Exception($"{label}: z={h.position.z}");
        if (h.skills.Length != 4) throw new Exception($"{label}: skills={h.skills.Length}");
        if (h.skills[3] != 100) throw new Exception($"{label}: skill[3]={h.skills[3]}");
        if (h.inventory[0].name != "Excalibur") throw new Exception($"{label}: sword={h.inventory[0].name}");
        if (h.inventory[0].value != 9999) throw new Exception($"{label}: val={h.inventory[0].value}");
        if (d.loot_table[0].name != "HealthPotion") throw new Exception($"{label}: potion");
        if (d.loot_table[0].rarity != "Common") throw new Exception($"{label}: rarity");
    }

    static void Main(string[] args) {
        Console.WriteLine("🟣 C#");

        // 1. Roundtrip
        var w = CreateTestData();
        byte[] encoded = w.Encode();
        Console.WriteLine($"   Encoded: {encoded.Length} bytes");

        var decoded = WorldState.Decode(encoded);
        Verify(decoded, "C# roundtrip");
        Console.WriteLine("   ✅ Roundtrip PASS");

        // 2. Write to file
        File.WriteAllBytes("test_data_csharp.bin", encoded);
        Console.WriteLine("   📁 Written to test_data_csharp.bin");

        // 3. Cross-language: decode Python's data
        if (File.Exists("test_data.bin")) {
            byte[] pyData = File.ReadAllBytes("test_data.bin");
            var pyDecoded = WorldState.Decode(pyData);
            Verify(pyDecoded, "C#←Python cross-lang");
            Console.WriteLine("   ✅ Cross-language decode (Python→C#) PASS");
        } else {
            Console.WriteLine("   ⚠️ No Python test_data.bin found");
        }
    }
}
