//! Cross-language conformance test for the Rust target, in both output
//! modes: single file and `--sep` (the mode the broker compiles). run.sh
//! generates everything under gen/ from the current generator first.
#![allow(dead_code, unused_imports, unused_mut, clippy::all)]

use std::path::Path;

mod bench {
    include!("../gen/bench/rust/vec3.rs");
}
mod bench_sep {
    include!("../gen/bench_sep/rust/vec3_structs.rs");
    include!("../gen/bench_sep/rust/vec3_impl.rs");
}
mod edge {
    include!("../gen/edge/rust/inner.rs");
}
mod edge_sep {
    include!("../gen/edge_sep/rust/inner_structs.rs");
    include!("../gen/edge_sep/rust/inner_impl.rs");
}

struct T {
    passed: u32,
    failed: u32,
}

impl T {
    fn check(&mut self, name: &str, ok: bool, detail: impl FnOnce() -> String) {
        if ok {
            self.passed += 1;
            println!("  ok   {name}");
        } else {
            self.failed += 1;
            println!("  FAIL {name} ({})", detail());
        }
    }

    fn truncations(&mut self, name: &str, data: &[u8], decode: impl Fn(&[u8]) -> bool) {
        for cut in 0..data.len() {
            if decode(&data[..cut]) {
                self.check(name, false, || format!("prefix of {cut}/{} bytes decoded", data.len()));
                return;
            }
        }
        self.check(name, true, String::new);
    }
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

fn bad_version(data: &[u8]) -> Vec<u8> {
    let mut b = data.to_vec();
    b[1] ^= 1;
    b
}

macro_rules! bench_checks {
    ($t:expr, $m:ident, $label:expr, $root:expr, $write:expr) => {{
        use $m::*;
        let t: &mut T = $t;
        let label: &str = $label;
        let make = || WorldState {
            world_id: 42,
            seed: "cross_lang_test".into(),
            guilds: vec![Guild {
                name: "TestGuild".into(),
                description: "A test guild for cross-language".into(),
                members: vec![Character {
                    name: "TestHero".into(),
                    level: 99,
                    hp: 1000,
                    mp: 500,
                    is_alive: true,
                    position: Vec3 { x: 10, y: -20, z: 30 },
                    skills: vec![1, 2, 3, 100],
                    inventory: vec![Item {
                        id: 1,
                        name: "Excalibur".into(),
                        value: 9999,
                        weight: 15,
                        rarity: "Legendary".into(),
                    }],
                }],
            }],
            loot_table: vec![Item {
                id: 2,
                name: "HealthPotion".into(),
                value: 50,
                weight: 1,
                rarity: "Common".into(),
            }],
        };
        let verify = |w: &WorldState| -> bool {
            let g = &w.guilds;
            w.world_id == 42
                && w.seed == "cross_lang_test"
                && g.len() == 1
                && g[0].name == "TestGuild"
                && g[0].description == "A test guild for cross-language"
                && g[0].members.len() == 1
                && {
                    let h = &g[0].members[0];
                    h.name == "TestHero"
                        && h.level == 99
                        && h.hp == 1000
                        && h.mp == 500
                        && h.is_alive
                        && (h.position.x, h.position.y, h.position.z) == (10, -20, 30)
                        && h.skills == vec![1, 2, 3, 100]
                        && h.inventory.len() == 1
                        && h.inventory[0].name == "Excalibur"
                        && h.inventory[0].value == 9999
                        && h.inventory[0].rarity == "Legendary"
                }
                && w.loot_table.len() == 1
                && w.loot_table[0].name == "HealthPotion"
                && w.loot_table[0].rarity == "Common"
        };
        let root: &Path = $root;
        let reference = std::fs::read(root.join("test_data.bin")).expect("test_data.bin");
        let enc = make().encode().expect("encode");
        if $write {
            std::fs::write(root.join("test_data_rust.bin"), &enc).expect("write");
        }
        t.check(&format!("{label} encode == test_data.bin"), enc == reference, || {
            format!("{} vs {} bytes", enc.len(), reference.len())
        });
        match WorldState::decode(&reference) {
            Ok(w) => t.check(&format!("{label} decode test_data.bin"), verify(&w), || format!("{w:?}")),
            Err(e) => t.check(&format!("{label} decode test_data.bin"), false, || e.to_string()),
        }
        match WorldState::decode(&enc) {
            Ok(w) => {
                t.check(&format!("{label} round-trip"), verify(&w), || format!("{w:?}"));
                let re = w.encode().expect("re-encode");
                t.check(&format!("{label} re-encode == encode"), re == enc, || "bytes differ".into());
            }
            Err(e) => t.check(&format!("{label} round-trip"), false, || e.to_string()),
        }
        t.check(
            &format!("{label} wrong version rejected"),
            WorldState::decode(&bad_version(&reference)).is_err(),
            || "decoded".into(),
        );
        t.truncations(&format!("{label} every truncation rejected"), &reference, |b| {
            WorldState::decode(b).is_ok()
        });
    }};
}

macro_rules! edge_checks {
    ($t:expr, $m:ident, $label:expr, $root:expr) => {{
        use $m::*;
        let t: &mut T = $t;
        let label: &str = $label;
        let make = || Edge {
            i_min: -2147483648,
            i_max: 2147483647,
            i_zero: 0,
            i_neg: -1,
            l_min: -9223372036854775808,
            l_max: 9223372036854775807,
            l_neg: -300,
            f: -1.25,
            d: 1234.5625,
            d_neg: -0.5,
            yes: true,
            no: false,
            empty: "".into(),
            unicode: "héllo wörld ✓ 日本 🚀".into(),
            ints: vec![0, -1, 1, -64, 64, -2147483648, 2147483647],
            longs: vec![0, -1, 9223372036854775807, -9223372036854775808, 4294967296],
            floats: vec![0.0, 0.5, -2.25],
            doubles: vec![0.0, 3.5, -1000000.25],
            bools: vec![true, false, true],
            strings: vec!["".into(), "a".into(), "日本語".into()],
            no_ints: vec![],
            inner: Inner { big: 1099511627776, label: "inner".into() },
            inners: vec![
                Inner { big: -1, label: "".into() },
                Inner { big: 0, label: "x".into() },
            ],
            no_inners: vec![],
        };
        let root: &Path = $root;
        let reference = std::fs::read(root.join("edge").join("edge_ref.bin")).expect("edge_ref.bin");
        let enc = make().encode().expect("encode");
        t.check(&format!("{label} encode == edge_ref.bin"), enc == reference, || format!("got {enc:02x?}"));
        match Edge::decode(&reference) {
            Ok(e) => {
                let w = make();
                macro_rules! field {
                    ($f:ident) => {
                        t.check(
                            &format!("{label} decode {}", stringify!($f)),
                            format!("{:?}", e.$f) == format!("{:?}", w.$f),
                            || format!("got {:?}, want {:?}", e.$f, w.$f),
                        );
                    };
                }
                field!(i_min); field!(i_max); field!(i_zero); field!(i_neg);
                field!(l_min); field!(l_max); field!(l_neg);
                t.check(&format!("{label} decode f"), e.f == w.f, || format!("{}", e.f));
                t.check(&format!("{label} decode d"), e.d == w.d, || format!("{}", e.d));
                t.check(&format!("{label} decode d_neg"), e.d_neg == w.d_neg, || format!("{}", e.d_neg));
                field!(yes); field!(no); field!(empty); field!(unicode);
                field!(ints); field!(longs);
                t.check(&format!("{label} decode floats"), e.floats == w.floats, || format!("{:?}", e.floats));
                t.check(&format!("{label} decode doubles"), e.doubles == w.doubles, || format!("{:?}", e.doubles));
                field!(bools); field!(strings); field!(no_ints);
                field!(inner); field!(inners); field!(no_inners);
                let re = e.encode().expect("re-encode");
                t.check(&format!("{label} re-encode == edge_ref.bin"), re == reference, || "bytes differ".into());
            }
            Err(e) => t.check(&format!("{label} decode edge_ref.bin"), false, || e.to_string()),
        }
        t.check(
            &format!("{label} wrong version rejected"),
            Edge::decode(&bad_version(&reference)).is_err(),
            || "decoded".into(),
        );
        t.truncations(&format!("{label} every truncation rejected"), &reference, |b| Edge::decode(b).is_ok());

        // float32 variant: x10000 must be computed in single precision
        let f32ref = std::fs::read(root.join("edge").join("edge_float32_ref.bin")).expect("edge_float32_ref.bin");
        let mut fv = make();
        fv.f = 0.29;
        fv.floats = vec![0.7, 16777.217, -0.29];
        let fenc = fv.encode().expect("encode float32 variant");
        t.check(&format!("{label} float32 encode == edge_float32_ref.bin"), fenc == f32ref, || format!("got {fenc:02x?}"));
        match Edge::decode(&f32ref) {
            Ok(e) => t.check(&format!("{label} float32 decode"), e.f == fv.f && e.floats == fv.floats, || {
                format!("{} {:?}", e.f, e.floats)
            }),
            Err(e) => t.check(&format!("{label} float32 decode"), false, || e.to_string()),
        }
        // trailing bytes after the root class are ignored
        let mut trailing = reference.clone();
        trailing.extend_from_slice(&[0x00, 0xff]);
        match Edge::decode(&trailing) {
            Ok(e) => t.check(&format!("{label} trailing bytes ignored"), e.encode().unwrap() == reference, || {
                "decoded value differs".into()
            }),
            Err(e) => t.check(&format!("{label} trailing bytes ignored"), false, || e.to_string()),
        }
        // unencodable floats are errors, not saturated
        let mut nan = make();
        nan.d = f64::NAN;
        t.check(&format!("{label} encode NaN double rejected"), nan.encode().is_err(), || "encoded".into());
        let mut big = make();
        big.f = 1e30;
        t.check(&format!("{label} encode out-of-range float rejected"), big.encode().is_err(), || "encoded".into());
    }};
}

fn main() {
    let root = std::env::args().nth(1).expect("usage: crosstest <cross_lang_test dir>");
    let root = Path::new(&root);
    let mut t = T { passed: 0, failed: 0 };
    bench_checks!(&mut t, bench, "bench", root, true);
    bench_checks!(&mut t, bench_sep, "bench --sep", root, false);
    edge_checks!(&mut t, edge, "edge", root);
    edge_checks!(&mut t, edge_sep, "edge --sep", root);
    // hostile input: bogus lengths must fail fast, not allocate or panic
    t.check("bench hostile huge array length rejected", bench::WorldState::decode(&unhex("0a312e302e300000feffffff0f")).is_err(), || "decoded".into());
    t.check("bench hostile negative array length rejected", bench::WorldState::decode(&unhex("0a312e302e30000001")).is_err(), || "decoded".into());
    t.check("edge hostile huge string length rejected", edge::Inner::decode(&unhex("0a322e312e30008080808010")).is_err(), || "decoded".into());
    t.check("edge hostile negative string length rejected", edge::Inner::decode(&unhex("0a322e312e300001")).is_err(), || "decoded".into());
    t.check("edge hostile endless varint rejected", edge::Inner::decode(&unhex("0a322e312e30ffffffffffffffffffffff")).is_err(), || "decoded".into());
    t.check("edge hostile invalid UTF-8 string rejected", edge::Inner::decode(&unhex("0a322e312e300002ff")).is_err(), || "decoded".into());
    t.check("edge --sep hostile huge string length rejected", edge_sep::Inner::decode(&unhex("0a322e312e30008080808010")).is_err(), || "decoded".into());
    t.check("edge --sep hostile negative string length rejected", edge_sep::Inner::decode(&unhex("0a322e312e300001")).is_err(), || "decoded".into());
    t.check("edge --sep hostile endless varint rejected", edge_sep::Inner::decode(&unhex("0a322e312e30ffffffffffffffffffffff")).is_err(), || "decoded".into());
    if t.failed > 0 {
        std::process::exit(1);
    }
}
