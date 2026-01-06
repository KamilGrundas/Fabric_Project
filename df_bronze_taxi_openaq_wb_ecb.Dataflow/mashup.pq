[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "378a3317-a99d-4eea-a9a8-d061cb17fc3f"]}[Data]{[lakehouseId = "f5ab1335-6f76-4c11-b8bf-40691650e9b2"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_openaq_locations_raw_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_openaq_locations_raw = let
    Json =
        Json.Document(
            Web.Contents(
                "https://api.openaq.org/v3/locations",
                [
                    Headers = [
                        #"X-API-Key" = "693bfc443c1eec76cade83db4067b367ad85234a50fc260f944213a54846af7b"
                    ],
                    Query = [
                        bbox = "-74.2591,40.4774,-73.7004,40.9176",
                        limit = "100",
                        page = "1"
                    ]
                ]
            )
        ),
 Results = Json[results],

    Table = Table.FromList(
        Results,
        Splitter.SplitByNothing(),
        {"raw_json"}
    ),

    RawJsonText = Table.TransformColumns(
        Table,
        {
            "raw_json",
            each Text.FromBinary(Json.FromValue(_), TextEncoding.Utf8)
        }
    ),
  #"Zmieniono typ kolumny" = Table.TransformColumnTypes(RawJsonText, {{"raw_json", type text}})
in
    #"Zmieniono typ kolumny";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_ecb_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_ecb = let
    Source =
        Csv.Document(
            Web.Contents(
                "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"
            ),
            [
                Delimiter = ",",
                Columns = 32,
                QuoteStyle = QuoteStyle.None
            ]
        ),

    PromotedHeaders =
        Table.PromoteHeaders(
            Source,
            [PromoteAllScalars = true]
        ),

    Typed =
        Table.TransformColumnTypes(
            PromotedHeaders,
            {
                {"KEY", type text},
                {"FREQ", type text},
                {"CURRENCY", type text},
                {"CURRENCY_DENOM", type text},
                {"EXR_TYPE", type text},
                {"EXR_SUFFIX", type text},
                {"TIME_PERIOD", type date},
                {"OBS_VALUE", type number},
                {"OBS_STATUS", type text},
                {"OBS_CONF", type text},
                {"OBS_PRE_BREAK", type text},
                {"OBS_COM", type text},
                {"TIME_FORMAT", type text},
                {"BREAKS", type text},
                {"COLLECTION", type text},
                {"COMPILING_ORG", type text},
                {"DISS_ORG", type text},
                {"DOM_SER_IDS", type text},
                {"PUBL_ECB", type text},
                {"PUBL_MU", type text},
                {"PUBL_PUBLIC", type text},
                {"UNIT_INDEX_BASE", type text},
                {"COMPILATION", type text},
                {"COVERAGE", type text},
                {"DECIMALS", Int64.Type},
                {"NAT_TITLE", type text},
                {"SOURCE_AGENCY", type text},
                {"SOURCE_PUB", type text},
                {"TITLE", type text},
                {"TITLE_COMPL", type text},
                {"UNIT", type text},
                {"UNIT_MULT", Int64.Type}
            },
            "en-US"
        )
in
    Typed;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_nyc_taxi_locations_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_nyc_taxi_locations = let
  Źródło = Csv.Document(Web.Contents("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"), [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
  #"Nagłówki o podwyższonym poziomie" = Table.PromoteHeaders(Źródło, [PromoteAllScalars = true]),
  #"Zmieniono typ kolumny" = Table.TransformColumnTypes(#"Nagłówki o podwyższonym poziomie", {{"LocationID", Int64.Type}, {"Borough", type text}, {"Zone", type text}, {"service_zone", type text}}, "pl")
in
  #"Zmieniono typ kolumny";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_worldbank_raw_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_worldbank_raw = let
    Source =
        Json.Document(
            Web.Contents(
                "https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json"
            )
        ),

    Records = Source{1},

    JsonRaw =
        List.Transform(
            Records,
            each Text.FromBinary(
                Json.FromValue(_),
                TextEncoding.Utf8
            )
        ),

    ToTable =
        Table.FromList(
            JsonRaw,
            Splitter.SplitByNothing(),
            {"raw_json"}
        ),

    Typed =
        Table.TransformColumnTypes(
            ToTable,
            {{"raw_json", type text}}
        )
in
    Typed;
shared bronze_openaq_locations_raw_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "9cd4ffe5-68b7-4261-8163-b56bf0bc32ea"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "5a8fce1b-a162-4453-8498-21708537897d"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_openaq_locations_raw", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared bronze_ecb_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "9cd4ffe5-68b7-4261-8163-b56bf0bc32ea"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "5a8fce1b-a162-4453-8498-21708537897d"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_ecb", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared bronze_nyc_taxi_locations_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "9cd4ffe5-68b7-4261-8163-b56bf0bc32ea"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "5a8fce1b-a162-4453-8498-21708537897d"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_nyc_taxi_locations", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared bronze_worldbank_raw_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "9cd4ffe5-68b7-4261-8163-b56bf0bc32ea"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "5a8fce1b-a162-4453-8498-21708537897d"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_worldbank_raw", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
