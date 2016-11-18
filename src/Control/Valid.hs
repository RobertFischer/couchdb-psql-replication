module Control.Valid (
  module Control.Valid
) where

import Control.Exception

validValues :: [ a ] -> IO [ a ]
validValues [] = return []
validValues (x:xs) = do
  t <- try (evaluate x)
  ys <- validValues xs
  case t of
    Right y -> return $ y : ys
    Left e -> let _ = (e :: SomeException) in return ys

