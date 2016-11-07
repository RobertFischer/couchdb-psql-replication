
module Control.Monad.Trans.Control.Fixed (
    module Orig,
    liftThroughBase
) where

    import Control.Monad.Trans.Control as Orig

    liftThroughBase :: MonadBaseControl b m =>
        (b (StM m x) -> b (StM m y)) -> m x -> m y
    liftThroughBase f act = do
        r <- liftBaseWith (\rib -> f (rib act))
        restoreM r

