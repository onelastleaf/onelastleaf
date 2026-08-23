module Echo (echo) where

import Data.Text (Text)
import qualified Data.Text as Text

echo :: [Text] -> Text
echo = Text.unwords
