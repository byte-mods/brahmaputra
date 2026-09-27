# frozen_string_literal: true

require_relative "lib/brahmaputra/version"

Gem::Specification.new do |spec|
  spec.name          = "brahmaputra"
  spec.version       = Brahmaputra::VERSION
  spec.summary       = "Native Ruby client for the Brahmaputra log broker"
  spec.description   = "Producer, consumer and consumer-group client speaking Brahmaputra's " \
                       "wire protocol directly. Standard library only."
  spec.authors       = ["Brahmaputra contributors"]
  spec.license       = "Apache-2.0"
  spec.homepage      = "https://github.com/byte-mods/brahmaputra"
  spec.required_ruby_version = ">= 3.0"
  spec.files         = Dir["lib/**/*.rb"] + ["README.md"]
  spec.require_paths = ["lib"]
end
