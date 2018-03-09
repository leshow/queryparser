module Main where

import           API
import           Control.Monad.IO.Class          (liftIO)
-- import           Control.Monad.Trans.Except
import           Data.Either
import           Data.Functor                    (void)
import qualified Data.HashMap.Strict             as HMS
import qualified Data.List                       as L
import qualified Data.Map                        as M
import           Data.Monoid                     ((<>))
import           Data.Proxy
import qualified Data.Set                        as S
import qualified Data.Text.Lazy                  as TL
import qualified Data.Vector                     as V
import           Database.Sql.Type
import           Database.Sql.Util.Columns
import           Database.Sql.Util.Joins
import           Database.Sql.Util.Lineage.Table
import           Database.Sql.Util.Scope         (runResolverWarn)
import           Database.Sql.Util.Tables
import qualified Database.Sql.Vertica.Parser     as VP
import           Database.Sql.Vertica.Type       (Vertica, VerticaStatement,
                                                  resolveVerticaStatement)
import           Network.Wai
import           Network.Wai.Handler.Warp
import           Servant

server :: Server QAPI
server = allH :<|> joinH :<|> tableH :<|> colH :<|> lineageH
  where
    joinH :: TL.Text -> Handler Join
    joinH input = pure Join {_joins = buildJoins input}

    allH :: TL.Text -> Handler All
    allH input = pure All
        { joins   = buildJoins input
        , tables  = buildTables input
        , columns = buildCols input
        , lineage = buildLineage input
        }
    tableH :: TL.Text -> Handler Table
    tableH input = pure API.Table {_tables = buildTables input}

    colH :: TL.Text -> Handler Column
    colH input = pure API.Column {_columns = buildCols input}

    lineageH :: TL.Text -> Handler Lineage
    lineageH input = pure Lineage {_lineage = buildLineage input}

runServer :: Application
runServer = serve qApi server

runApp :: Port -> IO ()
runApp port = run port runServer

main :: IO ()
main = runApp 3000


buildTables :: TL.Text -> [TL.Text]
buildTables sql = draw $ getTables $ fst $ parseAndResolve sql
  where
    draw :: S.Set FullyQualifiedTableName -> [TL.Text]
    draw xs = case S.toList xs of
        []  -> ["no tables accessed"]
        xs' -> map drawFQTN xs'

buildCols :: TL.Text -> [[TL.Text]]
buildCols sql = draw $ getColumns $ fst $ parseAndResolve sql
  where
    draw :: S.Set (FullyQualifiedColumnName, Clause) -> [[TL.Text]]
    draw xs = case S.toList xs of
        []  -> [["no columns accessed"]]
        xs' -> map drawCol xs'

    drawCol :: (FullyQualifiedColumnName, Clause) -> [TL.Text]
    drawCol (col, clause) = [drawFQCN col, clause]

buildJoins :: TL.Text -> [[TL.Text]]
buildJoins sql = draw $ getJoins $ fst $ parseAndResolve sql
  where
    draw
        :: S.Set
               ( (FullyQualifiedColumnName, [StructFieldName ()])
               , (FullyQualifiedColumnName, [StructFieldName ()])
               )
        -> [[TL.Text]]
    draw xs = case S.toList xs of
        []  -> [["no joins"]]
        xs' -> map drawJoin xs'

    drawJoin
        :: ( (FullyQualifiedColumnName, [StructFieldName ()])
           , (FullyQualifiedColumnName, [StructFieldName ()])
           )
        -> [TL.Text]
    drawJoin (f1, f2) = [drawField f1, drawField f2]

buildLineage :: TL.Text -> [TL.Text]
buildLineage sql = draw $ getTableLineage $ fst $ parseAndResolve sql
  where
    draw :: M.Map FQTN (S.Set FQTN) -> [TL.Text]
    draw xs = case M.assocs xs of
        []  -> ["no tables modified"]
        xs' -> map drawAssoc xs'

    drawAssoc :: (FQTN, S.Set FQTN) -> TL.Text
    drawAssoc (tgt, srcs) = case S.toList srcs of
        []    -> TL.concat ["no longer has data -> ", drawFQTN tgt]
        srcs' -> TL.concat [drawDeps srcs', "->", drawFQTN tgt]


    drawDeps :: [FQTN] -> TL.Text
    drawDeps srcs = TL.intercalate ", " $ map drawFQTN srcs


-- pretty printing helpers
drawFQTN :: FullyQualifiedTableName -> TL.Text
drawFQTN FullyQualifiedTableName { fqtnSchemaName, fqtnTableName } =
    TL.intercalate "." [fqtnSchemaName, fqtnTableName]

drawFQCN :: FullyQualifiedColumnName -> TL.Text
drawFQCN FullyQualifiedColumnName { fqcnSchemaName, fqcnTableName, fqcnColumnName }
    = TL.intercalate "." [fqcnSchemaName, fqcnTableName, fqcnColumnName]

drawField :: (FullyQualifiedColumnName, [StructFieldName ()]) -> TL.Text
drawField (fqcn, fields) = foldl1
    combineWithDot
    (drawFQCN fqcn : map drawStructFieldName fields)
  where
    combineWithDot x y = x <> "." <> y
    drawStructFieldName (StructFieldName _ name) = name


-- let's provide a really simple function to do parsing!
-- It will have ungraceful error handling.
parse :: TL.Text -> VerticaStatement RawNames ()
parse sql = case void <$> VP.parse sql of
    Right q   -> q
    Left  err -> error $ show err


-- let's provide a really simple function that combines parsing + resolving.
-- We'll hardcode the catalog and leave the error handling ungraceful, still.
parseAndResolve
    :: TL.Text -> (VerticaStatement ResolvedNames (), [ResolutionError ()])
parseAndResolve sql =
    case
            runResolverWarn (resolveVerticaStatement $ parse sql)
                            (Proxy :: Proxy Vertica)
                            cata
        of
            (Right queryResolved, resolutions) ->
                (queryResolved, lefts resolutions)
            (Left err, _) -> error $ show err

cata :: Catalog
cata = makeDefaultingCatalog catalogMap [defaultSchema] defaultDatabase
    where
    defaultDatabase :: DatabaseName ()
    defaultDatabase = DatabaseName () "defaultDatabase"

    defaultSchema :: UQSchemaName ()
    defaultSchema = mkNormalSchema "public" ()

    foo :: (UQTableName (), SchemaMember)
    foo = ( QTableName () None "", persistentTable [ QColumnName () None "" ] )

    bar :: (UQTableName (), SchemaMember)
    bar = ( QTableName () None "", persistentTable [ QColumnName () None "" ] )

    catalogMap :: CatalogMap
    catalogMap = HMS.singleton defaultDatabase $
                        HMS.fromList [ ( defaultSchema, HMS.fromList [ foo , bar ] ) ]
