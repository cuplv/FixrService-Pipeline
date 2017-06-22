defmodule Fixr-Pipeline.ComputeStep do
    use Mix.Config

    @spec startUp(String.t, atom, String.t) :: map
    def startUp(conFilePath, confAtom, prefix \\ "") do
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

    @spec incrementalCompute(map, fun, boolean, list) :: {atom, any}
    def incrementalCompute(confMap, function, exceptions \\ [], blocking \\ true) do
        url = confMap["DataWebService"]
        statMap = confMap["StatusMap"]
        notDone = confMap["NotDone"]
        err = "ErrorOn" <> notDone
        done = confMap["Done"]
        case Fixr-Pipeline.DWS.getAllKeysAndValues(url, statMap) do
            {:ok, listOfKeys, listOfValues} ->
                notDone = confMap["NotDone"]
                done = confMap["Done"]
                #Enum.map(listOfKeys, )
                listOfKeysAndValues = List.zip([listOfKeys, listOfValues])
                listOfTasks = List.foldl(listOfKeys, [], fn({key, stat}, acc) ->
                    case stat do
                        ^notDone ->
                            task = Task.async(Fixr-Pipeline.DoAFunction, :function, [confMap, function, key, ""])
                            [{task, key} | acc]
                        ^err ->
                            reason = Fixr-Pipeline.DWS.getValue(url, errMap, key)
                            isInTheList = List.foldl(exceptions, false, fn(exception, match) -> case {exception, match} do
                                    {_, true} -> true
                                    {^reason, false} -> true
                                    _ -> false
                                end
                            end)
                            case isInTheList do
                                true ->
                                    task = Task.async(Fixr-Fixr-Pipeline.DoAFunction, :function, [confMap, function, key, ""])
                                    [{task, key} | acc]
                                false -> acc
                            end
                        _ -> acc #Assume that it's either done or on another step.
                        #{:err, reason} -> {:err, reason}
                    end
                end)
                if (blocking) do
                    List.foldl(listOfTasks, {:ok, []}, fn(taskTuple, status) ->
                        {task, key} = taskTuple
                        case Task.await(task, 10*60*1000) do #Worst case scenario, a function should only take 10 hours.
                            {:ok, idB} -> status
                            {:error, reason} -> case status do
                                {:err, list} -> {:err, list++[{key, reason}]}
                                _ -> {:err, [{key, reason}]}
                            end
                            _ -> case status do
                                {:err, list} -> {:err, list++[{key, "Timed Out"}]}
                                 _ -> {:err, [{key, "Timed Out"}]}
                            end
                        end
                    end)
                else
                    {:ok, listOfTasks}
                end
            {:error, reason, _} -> {:error, reason}
        end
    end

    @spec checkIfValidConfFile(map) :: {atom, string}
    def checkIfValidConfFile(confMap) do
        case confMap["DataWebService"] do
            nil -> {:err, "This is not a valid configMap.\nPlease make sure you do startUp before you try to IncrementalCompute."}
            _ -> {:ok, ""}
        end
    end

    @spec computeStep(String.t, atom, String.t, fun, list, boolean) :: {atom, any}
    def computeStep(conFilePath, confAtom, function, prefix \\ "", exceptions \\ [], blocking \\ true) do
        confMap = startUp(conFilePath, confAtom, prefix)
        incrementalCompute(confMap, function, exceptions, blocking)
    end

end