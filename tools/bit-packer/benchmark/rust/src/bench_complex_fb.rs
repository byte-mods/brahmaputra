use crate::bench_complex_generated::bench_fb::{
    self, CharacterArgs, GuildArgs, ItemArgs, Vec3, WorldStateArgs,
};
use crate::bench_complex_structs::{
    Character as BpCharacter, Guild as BpGuild, Item as BpItem, Vec3 as BpVec3,
    WorldState as BpWorldState,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

pub fn encode(world: &BpWorldState) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();

    // Create Loot Table
    let mut loot_items = Vec::new();
    for item in &world.loot_table {
        let name = builder.create_string(&item.name);
        let rarity = builder.create_string(&item.rarity);
        let item_offset = bench_fb::Item::create(
            &mut builder,
            &ItemArgs {
                id: item.id,
                name: Some(name),
                value: item.value,
                weight: item.weight,
                rarity: Some(rarity),
            },
        );
        loot_items.push(item_offset);
    }
    let loot_table_vec = builder.create_vector(&loot_items);

    // Create Guilds
    let mut guild_offsets = Vec::new();
    for guild in &world.guilds {
        let name = builder.create_string(&guild.name);
        let description = builder.create_string(&guild.description);

        let mut member_offsets = Vec::new();
        for char in &guild.members {
            let char_name = builder.create_string(&char.name);

            let skills_vec = builder.create_vector(&char.skills);

            let mut inv_offsets = Vec::new();
            for item in &char.inventory {
                let i_name = builder.create_string(&item.name);
                let i_rarity = builder.create_string(&item.rarity);
                let i_off = bench_fb::Item::create(
                    &mut builder,
                    &ItemArgs {
                        id: item.id,
                        name: Some(i_name),
                        value: item.value,
                        weight: item.weight,
                        rarity: Some(i_rarity),
                    },
                );
                inv_offsets.push(i_off);
            }
            let inventory_vec = builder.create_vector(&inv_offsets);

            // Vec3 is a struct, not a table, so we create it inline or via create
            // It seems generated code has a `new` method for struct Vec3?
            // Wait, Vec3 is a struct in FBS.
            // In Rust generated code for Struct, we usually pass reference or use specialized create?
            // Let's check generated code.
            // Structs are usually passed as &Vec3.
            // We need to construct a Vec3 struct.
            // bench_fb::Vec3::new(x, y, z)
            let pos = bench_fb::Vec3::new(char.position.x, char.position.y, char.position.z);

            let char_off = bench_fb::Character::create(
                &mut builder,
                &CharacterArgs {
                    name: Some(char_name),
                    level: char.level,
                    hp: char.hp,
                    mp: char.mp,
                    is_alive: char.is_alive,
                    position: Some(&pos),
                    skills: Some(skills_vec),
                    inventory: Some(inventory_vec),
                },
            );
            member_offsets.push(char_off);
        }
        let members_vec = builder.create_vector(&member_offsets);

        let guild_off = bench_fb::Guild::create(
            &mut builder,
            &GuildArgs {
                name: Some(name),
                description: Some(description),
                members: Some(members_vec),
            },
        );
        guild_offsets.push(guild_off);
    }
    let guilds_vec = builder.create_vector(&guild_offsets);

    let seed = builder.create_string(&world.seed);

    let world_off = bench_fb::WorldState::create(
        &mut builder,
        &WorldStateArgs {
            world_id: world.world_id,
            seed: Some(seed),
            guilds: Some(guilds_vec),
            loot_table: Some(loot_table_vec),
        },
    );

    builder.finish(world_off, None);
    builder.finished_data().to_vec()
}

pub fn decode(data: &[u8]) -> BpWorldState {
    let fb_world = bench_fb::root_as_world_state(data).unwrap();

    let mut world = BpWorldState {
        world_id: fb_world.world_id(),
        seed: fb_world.seed().unwrap_or("").to_string(),
        guilds: Vec::new(),
        loot_table: Vec::new(),
    };

    if let Some(fb_guilds) = fb_world.guilds() {
        for fb_guild in fb_guilds {
            let mut guild = BpGuild {
                name: fb_guild.name().unwrap_or("").to_string(),
                description: fb_guild.description().unwrap_or("").to_string(),
                members: Vec::new(),
            };

            if let Some(fb_members) = fb_guild.members() {
                for fb_char in fb_members {
                    let mut char = BpCharacter {
                        name: fb_char.name().unwrap_or("").to_string(),
                        level: fb_char.level(),
                        hp: fb_char.hp(),
                        mp: fb_char.mp(),
                        is_alive: fb_char.is_alive(),
                        position: BpVec3 { x: 0, y: 0, z: 0 }, // Default
                        skills: Vec::new(),
                        inventory: Vec::new(),
                    };

                    if let Some(pos) = fb_char.position() {
                        char.position = BpVec3 {
                            x: pos.x(),
                            y: pos.y(),
                            z: pos.z(),
                        };
                    }

                    if let Some(skills) = fb_char.skills() {
                        char.skills = skills.iter().collect();
                    }

                    if let Some(inv) = fb_char.inventory() {
                        for item in inv {
                            char.inventory.push(BpItem {
                                id: item.id(),
                                name: item.name().unwrap_or("").to_string(),
                                value: item.value(),
                                weight: item.weight(),
                                rarity: item.rarity().unwrap_or("").to_string(),
                            });
                        }
                    }
                    guild.members.push(char);
                }
            }
            world.guilds.push(guild);
        }
    }

    if let Some(loot) = fb_world.loot_table() {
        for item in loot {
            world.loot_table.push(BpItem {
                id: item.id(),
                name: item.name().unwrap_or("").to_string(),
                value: item.value(),
                weight: item.weight(),
                rarity: item.rarity().unwrap_or("").to_string(),
            });
        }
    }

    world
}
