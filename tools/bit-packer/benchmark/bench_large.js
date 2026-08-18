const fs = require('fs');
const path = require('path');
const { WorldState } = require('./generated/js_bench/js/vec3');

const fileName = 'large_payload.bin';
let filePath = path.join(__dirname, '..', fileName);

if (!fs.existsSync(filePath)) {
    console.error(`File not found: ${filePath}`);
    process.exit(1);
}

const data = fs.readFileSync(filePath);
console.log(`Read ${data.length} bytes from ${fileName}`);

// Decode
const startDecode = process.hrtime();
const world = WorldState.decode(data);
const endDecode = process.hrtime(startDecode);
const elapsedDecode = endDecode[0] + endDecode[1] / 1e9;
console.log(`Decode time: ${elapsedDecode.toFixed(4)}s`);

console.log(`Decoded ${world.guilds.length} guilds`);

// Encode
const startEncode = process.hrtime();
const encodedData = world.encode();
const endEncode = process.hrtime(startEncode);
const elapsedEncode = endEncode[0] + endEncode[1] / 1e9;
console.log(`Encode time: ${elapsedEncode.toFixed(4)}s`);
console.log(`Encoded size: ${encodedData.length} bytes`);

if (encodedData.length !== data.length) {
    console.log(`WARNING: Encoded size mismatch! Original: ${data.length}, New: ${encodedData.length}`);
}
