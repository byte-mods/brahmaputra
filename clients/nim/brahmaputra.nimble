# Package

version       = "0.1.0"
author        = "Brahmaputra contributors"
description   = "Native client for the Brahmaputra log broker: producer, consumer and consumer groups"
license       = "Apache-2.0"
srcDir        = "src"

# Dependencies

requires "nim >= 1.6.0"

task e2e, "Run the end-to-end suite: nimble e2e (BROKER=host:port)":
  let broker = getEnv("BROKER", "127.0.0.1:9092")
  exec "nim c -d:release --threads:on --mm:orc --hints:off --outdir:build -r tests/manual_test.nim " & broker
