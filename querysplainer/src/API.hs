module API where

import           Data.Aeson
import           Data.Proxy
import qualified Data.Text.Lazy as TL
import           GHC.Generics
import           Servant.API


type API = "all" :> ReqBody '[PlainText] TL.Text :> Post '[JSON] All
        :<|> "joins" :> ReqBody '[PlainText] TL.Text :> Post '[JSON] Join
        :<|> "tables" :> ReqBody '[PlainText] TL.Text :>  Post '[JSON] Table
        :<|> "columns" :> ReqBody '[PlainText] TL.Text :> Post '[JSON] Column
        :<|> "lineage" :> ReqBody '[PlainText] TL.Text :> Post '[JSON] Lineage

qApi :: Proxy API
qApi = Proxy

data All = All {
    joins     :: [[TL.Text]]
    , tables  :: [TL.Text]
    , columns :: [[TL.Text]]
    , lineage :: [TL.Text]
} deriving (Eq, Show, Generic)

newtype Join = Join {
    _joins :: [[TL.Text]]
} deriving (Eq, Show, Generic)

newtype Table = Table {
    _tables :: [TL.Text]
} deriving (Eq, Show, Generic)

newtype Column = Column {
    _columns :: [[TL.Text]]
} deriving (Eq, Show, Generic)

newtype Lineage = Lineage {
    _lineage :: [TL.Text]
} deriving (Eq, Show, Generic)

instance ToJSON All where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON All where

instance FromJSON Join where
    parseJSON = withObject "Join" $ \v -> Join
        <$> v .: "joins"

instance ToJSON Join where
    toJSON (Join _joins) = object ["joins" .= _joins]

instance FromJSON Table where
    parseJSON = withObject "Table" $ \v -> Table
        <$> v .: "tables"

instance ToJSON Table where
    toJSON (Table _tables) = object ["tables" .= _tables]

instance FromJSON Column where
    parseJSON = withObject "Column" $ \v -> Column
        <$> v .: "_columns"

instance ToJSON Column where
    toJSON (Column _columns) = object ["columns" .= _columns]

instance FromJSON Lineage where
    parseJSON = withObject "Lineage" $ \v -> Lineage
        <$> v .: "_lineage"






