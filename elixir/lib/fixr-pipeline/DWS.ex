defmodule Fixr-Pipeline.DWS do
    @spec databaseIsUp(String.t) :: boolean
    def databaseIsUp(probableURL) do
        case HTTPoison.get(probableURL) do
            {:ok, response} -> true
            {:error, reason} -> false
        end
    end

    @spec getAllKeys(String.t, String.t) :: {atom, any}
    def getAllKeys(dataWebService, dataMap) do
        case HTTPoison.get(dataWebService <> "getAllKeys?dataMap=" <> dataMap) do
            {:ok, response} ->
                mp = Poison.decode!(response.body)
                case mp["succ"] do
                    true -> {:ok, mp["keys"]}
                    _ -> {:error, "Failed to load the " <> dataMap <> " map."}
                end
            _ -> {:error, "Failed to load the" <> dataMap <> " map."}
        end
    end

    @spec getAllKeysAndValues(String.t, String.t) :: {atom, any, any}
    def getAllKeys(dataWebService, dataMap) do
        case HTTPoison.get(dataWebService <> "getAllKeys?values=true&dataMap=" <> dataMap) do
            {:ok, response} ->
                mp = Poison.decope!(response.body)
                case mp["succ"] do
                    true -> {:ok, mp["keys"]}
                    _ -> {:error, "Failed to load the " <> dataMap <> " map.", ""}
                end
            _ -> {:error, "Failed to load the " <> dataMap <> " map.", ""}
        end
    end

    @spec getValue(String.t, String.t, String.t) :: {atom, any}
    def getValue(dataWebService, dataMap, id) do
        case HTTPoison.get(dataWebService <> "get?dataMap=" <> dataMap <> "&key=" <> id) do
            {:ok, response} ->
                mp = Poison.decode!(response.body)
                case mp["succ"] do
                    true -> {:ok, mp["value"]}
                    _ -> {:error, "Failed to load the " <> id <> " value in the " <> dataMap <> " map."}
                end
            _ -> {:error, "Failed to load the " <> dataMap <> " map."}
        end
    end

    @spec putValue(String.t, String.t, String.t, any) :: {atom, String.t}
    def putValue(dataWebService, dataMap, id, value) do
        json = Poison.encode!(%{"key" => id, "value" => value, "dataMap" => dataMap})
        case HTTPoison.post(dataWebService, json) do
            {:ok, response} ->
                mp = Poison.decode!(response.body)
                case mp["succ"] do
                    true -> {:ok, ""}
                    _ -> {:error, "Failed to put the data in."}
                end
            _ -> {:error, "Failed to load the " <> dataMap <> " map."}
        end
    end
end