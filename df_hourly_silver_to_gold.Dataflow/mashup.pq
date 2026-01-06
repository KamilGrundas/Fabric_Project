[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = let
  Source = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation = Source{[workspaceId = "9cd4ffe5-68b7-4261-8163-b56bf0bc32ea"]}[Data],
  #"Nawigacja 1" = Navigation{[warehouseId = "8e4f068d-5e47-4f26-a593-3ea9f10014ec"]}[Data]
in
  #"Nawigacja 1";
[BindToDefaultDestination = true]
shared silver_sensor_measurements_hourly = let
  Źródło = Lakehouse.Contents(null),
  Nawigacja = Źródło{[workspaceId = "9cd4ffe5-68b7-4261-8163-b56bf0bc32ea"]}[Data],
  #"Nawigacja 1" = Nawigacja{[lakehouseId = "5a8fce1b-a162-4453-8498-21708537897d"]}[Data],
  #"Nawigacja 2" = #"Nawigacja 1"{[Id = "silver_sensor_measurements_hourly", ItemKind = "Table"]}[Data]
in
  #"Nawigacja 2";
