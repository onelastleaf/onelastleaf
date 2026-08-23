module Main (main) where

import Echo (echo)

main :: IO ()
main = if echo ["one", "two"] == "one two" then pure () else fail "echo mismatch"
