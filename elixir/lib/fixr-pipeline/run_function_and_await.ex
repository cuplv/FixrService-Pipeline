defmodule RunFunctionAndAwait do
    #@moduledoc false
    @spec runAFunction(map, fun, String.t, String.t) :: {atom, any}
    def runAFunction(conFile, function, key, prefixB) do
        url = confMap["DataWebService"]
        idToA = conFile["IDToAMap"]
        statMap = conFile["StatusMap"]
        notDone = conFile["NotDone"]
        done = conFile["Done"]
        idToB = conFile["IDToBMap"]
        errMap = conFile["ErrorMap"]
        provMap = conFile["ProvenanceMap"]
        aToBMap = conFile["AToBMap"]
        a = Fixr-Pipeline.DWS.getValue(url, idToA, function, key)
        func = spawn(Fixr-Pipeline.DoAFunction, :function, [])
        send(func, {function, a, self})
        recieve do
            {:ok, b} ->
                #First, we need to generate an ID for B.
                idB = prefixB <> key #To be changed later. :\
                Fixr-Pipeline.DWS.putValue(url, idToB, idB, b)
                Fixr-Pipeline.DWS.putValue(url, provMap, idB, [key])
                Fixr-Pipeline.DWS.putValue(url, statMap, key, done)
                Fixr-Pipeline.DWS.putValue(url, aToBMap, key, idB)
                {:ok, idB}
            {:error, reason} ->
                Fixr-Pipeline.DWS.putValue(url, statMap, "ErrorOn" <> notDone)
                Fixr-Pipeline.DWS.putValue(url, errMap, reason)
                {:error, reason}
            _ ->
                Fixr-Pipeline.DWS.putValue(url, statMap, "ErrorOn" <> notDone)
                Fixr-Pipeline.DWS.putValue(url, errMap, "Unknown reason.")
                {:error, "Unknown Reason"}
        end
    end
end