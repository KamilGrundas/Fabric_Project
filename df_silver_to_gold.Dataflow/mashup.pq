[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "9cd4ffe5-68b7-4261-8163-b56bf0bc32ea"]}[Data]{[warehouseId = "8e4f068d-5e47-4f26-a593-3ea9f10014ec"]}[Data];
[BindToDefaultDestination = true]
shared bronze_nyc_taxi_locations = let
  Źródło = Lakehouse.Contents(null),
  Nawigacja = Źródło{[workspaceId = "9cd4ffe5-68b7-4261-8163-b56bf0bc32ea"]}[Data],
  #"Nawigacja 1" = Nawigacja{[lakehouseId = "5a8fce1b-a162-4453-8498-21708537897d"]}[Data],
  #"Nawigacja 2" = #"Nawigacja 1"{[Id = "bronze_nyc_taxi_locations", ItemKind = "Table"]}[Data]
in
  #"Nawigacja 2";
[BindToDefaultDestination = true]
shared silver_taxi_trips = let
  Źródło = Lakehouse.Contents(null),
  Nawigacja = Źródło{[workspaceId = "9cd4ffe5-68b7-4261-8163-b56bf0bc32ea"]}[Data],
  #"Nawigacja 1" = Nawigacja{[lakehouseId = "5a8fce1b-a162-4453-8498-21708537897d"]}[Data],
  #"Nawigacja 2" = #"Nawigacja 1"{[Id = "silver_taxi_trips", ItemKind = "Table"]}[Data]
in
  #"Nawigacja 2";
[BindToDefaultDestination = true]
shared silver_openaq_locations = let
  Źródło = Lakehouse.Contents(null),
  Nawigacja = Źródło{[workspaceId = "9cd4ffe5-68b7-4261-8163-b56bf0bc32ea"]}[Data],
  #"Nawigacja 1" = Nawigacja{[lakehouseId = "5a8fce1b-a162-4453-8498-21708537897d"]}[Data],
  #"Nawigacja 2" = #"Nawigacja 1"{[Id = "silver_openaq_locations", ItemKind = "Table"]}[Data]
in
  #"Nawigacja 2";
[BindToDefaultDestination = true]
shared silver_sensors = let
  Źródło = Lakehouse.Contents(null),
  Nawigacja = Źródło{[workspaceId = "9cd4ffe5-68b7-4261-8163-b56bf0bc32ea"]}[Data],
  #"Nawigacja 1" = Nawigacja{[lakehouseId = "5a8fce1b-a162-4453-8498-21708537897d"]}[Data],
  #"Nawigacja 2" = #"Nawigacja 1"{[Id = "silver_taxi_trips", ItemKind = "Table"]}[Data]
in
  #"Nawigacja 2";
