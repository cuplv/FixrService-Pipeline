defmodule Fixr-Pipeline.ComputeStep do
    use Mix.Config

    def startUp(conFilePath, confAtom, prefix) do
        import_config conFilePath
        %{"DataWebService" => Application.get_env(confAtom, prefix <> "WebServiceLocation", "http://localhost:8080/")}
        |> Map.put("StatusMap", Application.get_env(confAtom, prefix <> "StatusMapName", prefix <> "Status"))
        |> Map.put("NotDone", Application.get_env(confAtom, prefix <> "StatusNotDone", "Not Done"))
        |> Map.put("Done", Application.get_env(confAtom, prefix <> "StatusDone", "Done"))
        |> Map.put("IDToAMap", Application.get_env(confAtom, prefix <> "IDToAName", prefix <> "IDToA"))
        |> Map.put("IDToBMap", Application.get_env(confAtom, prefix <> "IDToBName", prefix <> "IDToB"))
        |> Map.put("ErrorMap", Application.get_env(confAtom, prefix <> "ErrorMap", prefix <> "Error"))
        |> Map.put("ProvenanceMap", Application.get_env(confAtom, prefix <> "ProvenanceMap", prefix <> "Provenance"))
        |> Map.put("AToBMap", Application.get_env(confAtom, prefix <> "AToBMap", "aToB"))
    end

    def incrementalCompute(confMap, blocking, exceptions \\ []) do
        case Fixr-Pipeline.DWS.getAllKeys(confMap["DataWebService"], confMap["StatusMap"]) do
            {:ok, listOfKeys} ->
                notDone = confMap["NotDone"]
                done = confMap["Done"]
                #Enum.map(listOfKeys, )
                List.foldl(listOfKeys, :ok, fn(x, acc) ->

                end)
                {:ok, :yourenotdoneyet}
            {:error, reason} ->
                {:error, reason}
        end
    end

end