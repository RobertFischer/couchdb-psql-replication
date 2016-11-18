{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}

module Util where

import Data.Maybe ( Maybe(..) )
import qualified Data.Char as Char

type Action = IO ()

removeChar :: Char -> String -> String
-- ^Removes the given character from the string.
removeChar _ []  = []
removeChar c (x:xs)
    | c == x = doRemove xs
    | otherwise = x : (doRemove xs)
  where
    doRemove = removeChar c

removeQuotes :: String -> String
-- ^Removes single and double quotes from the string
removeQuotes = (removeChar '"') . (removeChar '\'')

isBlank :: String -> Bool
-- ^Returns 'True' if the string is empty or only whitespace
isBlank [] = True
isBlank xs = all Char.isSpace xs

stripBlank :: String -> Maybe String
-- ^Returns 'Nothing' if the string 'isBlank'; otherwise, returns the string.
stripBlank str
  | isBlank str = Nothing
  | otherwise = Just str
