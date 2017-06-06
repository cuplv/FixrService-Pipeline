defmodule Fixr-Pipeline.DWS do
    def databaseIsUp(probableURL) do
        case HTTPoison.get(probableURL) do
            {:ok, response} -> true
            {:error, reason} -> false
        end
    end

    def getAllKeys(dataWebService, dataMap) do
        case HTTPoison.get(dataWebService <> "getAllKeys?dataMap=" <> dataMap) do
            {:ok, response} ->
            mp = Poison.decode!(response)
            case mp["succ"] do
                true -> {:ok, mp["keys"]}
                false -> {:error, "Failed to load the " <> dataMap <> " map."}
                    end
            _ -> {:error, "Failed to load the" <> dataMap <> " map."}
        end
    end

    def getValue(dataWebService, dataMap, id) do
        case HTTPoison.get(dataWebService <> "get?dataMap=" <> dataMap <> "&key=" <> id) do
            {:ok, response} ->
                mp = Poison.decode!(response)
                case mp["succ"] do
                    true -> {:ok, :yourenotdoneyet}
                    false -> {:error, "Failed to load the " <> id <> " value in the " <> dataMap <> " map."}
                end
            _ -> {:error, "Failed to load the " <> dataMap <> " map."}
        end
    end

end