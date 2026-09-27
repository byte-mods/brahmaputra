/// SCRAM-SHA-256 client proof (RFC 5802/7677), with SHA-256, HMAC and
/// PBKDF2 written out because dart:core ships none of them and this package
/// takes no dependencies.
library;

import 'dart:convert';
import 'dart:typed_data';

const List<int> _k = [
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
  0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
  0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
  0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
  0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
  0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
  0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
  0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

int _rotr(int x, int n) => ((x >>> n) | (x << (32 - n))) & 0xffffffff;

/// SHA-256 of [message].
Uint8List sha256(List<int> message) {
  final h = <int>[
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
  ];
  final bitLen = message.length * 8;
  final padded = BytesBuilder()
    ..add(message)
    ..addByte(0x80);
  while (padded.length % 64 != 56) {
    padded.addByte(0);
  }
  final lenBytes = ByteData(8)..setUint64(0, bitLen);
  padded.add(lenBytes.buffer.asUint8List());
  final data = ByteData.sublistView(padded.toBytes());
  final w = List<int>.filled(64, 0);
  for (var chunk = 0; chunk < data.lengthInBytes; chunk += 64) {
    for (var i = 0; i < 16; i++) {
      w[i] = data.getUint32(chunk + i * 4);
    }
    for (var i = 16; i < 64; i++) {
      final s0 = _rotr(w[i - 15], 7) ^ _rotr(w[i - 15], 18) ^ (w[i - 15] >>> 3);
      final s1 = _rotr(w[i - 2], 17) ^ _rotr(w[i - 2], 19) ^ (w[i - 2] >>> 10);
      w[i] = (w[i - 16] + s0 + w[i - 7] + s1) & 0xffffffff;
    }
    var a = h[0], b = h[1], c = h[2], d = h[3];
    var e = h[4], f = h[5], g = h[6], hh = h[7];
    for (var i = 0; i < 64; i++) {
      final s1 = _rotr(e, 6) ^ _rotr(e, 11) ^ _rotr(e, 25);
      final ch = (e & f) ^ ((~e & 0xffffffff) & g);
      final t1 = (hh + s1 + ch + _k[i] + w[i]) & 0xffffffff;
      final s0 = _rotr(a, 2) ^ _rotr(a, 13) ^ _rotr(a, 22);
      final maj = (a & b) ^ (a & c) ^ (b & c);
      final t2 = (s0 + maj) & 0xffffffff;
      hh = g;
      g = f;
      f = e;
      e = (d + t1) & 0xffffffff;
      d = c;
      c = b;
      b = a;
      a = (t1 + t2) & 0xffffffff;
    }
    h[0] = (h[0] + a) & 0xffffffff;
    h[1] = (h[1] + b) & 0xffffffff;
    h[2] = (h[2] + c) & 0xffffffff;
    h[3] = (h[3] + d) & 0xffffffff;
    h[4] = (h[4] + e) & 0xffffffff;
    h[5] = (h[5] + f) & 0xffffffff;
    h[6] = (h[6] + g) & 0xffffffff;
    h[7] = (h[7] + hh) & 0xffffffff;
  }
  final out = ByteData(32);
  for (var i = 0; i < 8; i++) {
    out.setUint32(i * 4, h[i]);
  }
  return out.buffer.asUint8List();
}

/// HMAC-SHA-256.
Uint8List hmacSha256(List<int> key, List<int> message) {
  var k = key.length > 64 ? sha256(key) : Uint8List.fromList(key);
  final block = Uint8List(64)..setRange(0, k.length, k);
  final inner = BytesBuilder()
    ..add([for (final b in block) b ^ 0x36])
    ..add(message);
  final outer = BytesBuilder()
    ..add([for (final b in block) b ^ 0x5c])
    ..add(sha256(inner.toBytes()));
  return sha256(outer.toBytes());
}

/// PBKDF2-HMAC-SHA-256 with a 32-byte output (one block).
Uint8List pbkdf2Sha256(List<int> password, List<int> salt, int iterations) {
  var u = hmacSha256(password, [...salt, 0, 0, 0, 1]);
  final result = Uint8List.fromList(u);
  for (var i = 1; i < iterations; i++) {
    u = hmacSha256(password, u);
    for (var j = 0; j < result.length; j++) {
      result[j] ^= u[j];
    }
  }
  return result;
}

/// One `key=value` field out of a SCRAM message.
String? scramField(String message, String key) {
  for (final part in message.split(',')) {
    if (part.startsWith('$key=')) return part.substring(key.length + 1);
  }
  return null;
}

/// The client proof: knowledge of the password without sending it.
String scramClientProof(
    String password, String salt, int iterations, String authMessage) {
  final salted =
      pbkdf2Sha256(utf8.encode(password), base64.decode(salt), iterations);
  final clientKey = hmacSha256(salted, utf8.encode('Client Key'));
  final storedKey = sha256(clientKey);
  final signature = hmacSha256(storedKey, utf8.encode(authMessage));
  return base64.encode(
      [for (var i = 0; i < clientKey.length; i++) clientKey[i] ^ signature[i]]);
}
