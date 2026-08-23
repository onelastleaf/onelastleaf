module Main (main) where

import Echo (echo)
import qualified Data.Text as Text
import Onelastleaf.PluginSDK

main :: IO ()
main = either (fail . Text.unpack) runPlugin $ do
  plugin <- newPlugin "__OLL_PLUGIN_ID__" "0.1.0"
  addAction
    (Action "echo" "Return the supplied arguments" $ \_ arguments ->
      pure (stringResult (echo arguments)))
    plugin
