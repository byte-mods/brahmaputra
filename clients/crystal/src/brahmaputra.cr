# Brahmaputra client driver for Crystal: producer, partition consumer and
# consumer groups over Brahmaputra's native wire protocol. Standard library
# only.
module Brahmaputra
  VERSION = "0.1.0"
end

require "./brahmaputra/errors"
require "./brahmaputra/protocol"
require "./brahmaputra/record_batch"
require "./brahmaputra/connection"
require "./brahmaputra/producer"
require "./brahmaputra/consumer"
require "./brahmaputra/assignors"
require "./brahmaputra/group_consumer"
