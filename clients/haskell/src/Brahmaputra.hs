-- | Brahmaputra client: producer, partition consumer and consumer groups.
--
-- > import qualified Brahmaputra as B
--
-- Every failure is thrown as 'BrahmaputraError'. Build with @-threaded@:
-- the linger and heartbeat threads rely on the threaded runtime.
module Brahmaputra
  ( module Brahmaputra.Protocol
  , module Brahmaputra.Connection
  , module Brahmaputra.Producer
  , module Brahmaputra.Consumer
  , module Brahmaputra.Group
  , module Brahmaputra.Assignor
  ) where

import Brahmaputra.Assignor
import Brahmaputra.Connection
import Brahmaputra.Consumer
import Brahmaputra.Group
import Brahmaputra.Producer
import Brahmaputra.Protocol
