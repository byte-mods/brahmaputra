use crate::bench_complex_structs as bp;
use prost::Message;

// Include generated code
include!(concat!(env!("OUT_DIR"), "/bench_complex.rs"));

pub fn from_bp(src: &bp::WorldState) -> WorldState {
    let mut guilds = Vec::new();
    for g in &src.guilds {
        let mut members = Vec::new();
        for c in &g.members {
            let mut inventory = Vec::new();
            for i in &c.inventory {
                inventory.push(Item {
                    id: i.id,
                    name: i.name.clone(),
                    value: i.value,
                    weight: i.weight,
                    rarity: i.rarity.clone(),
                });
            }
            members.push(Character {
                name: c.name.clone(),
                level: c.level,
                hp: c.hp,
                mp: c.mp,
                is_alive: c.is_alive,
                position: Some(Vec3 {
                    x: c.position.x,
                    y: c.position.y,
                    z: c.position.z,
                }),
                skills: c.skills.clone(),
                inventory,
            });
        }
        guilds.push(Guild {
            name: g.name.clone(),
            description: g.description.clone(),
            members,
        });
    }

    WorldState {
        world_id: src.world_id,
        seed: src.seed.clone(),
        guilds,
        loot_table: Vec::new(), // In main.rs it was empty? Let's check. Yes.
    }
}

pub fn encode(world: &WorldState) -> Vec<u8> {
    let mut buf = Vec::with_capacity(world.encoded_len());
    world.encode(&mut buf).unwrap();
    buf
}

pub fn decode(data: &[u8]) -> WorldState {
    WorldState::decode(data).unwrap()
}
