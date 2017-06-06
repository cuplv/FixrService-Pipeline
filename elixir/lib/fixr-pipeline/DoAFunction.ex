defmodule Fixr-Pipeline.DoAFunction do
    def loop do
        receive do
            {:function, aToB, a, sender} ->
                case aToB(a) do
                    {:ok, b} ->
                        send(sender, {:ok, b})
                    {:error, reason} ->
                        send(sender, {:error, reason})
                    b ->
                        send(sender, {:ok, b})
                end
            #{:program, path, a, sender} ->
            #
        end
    end
    loop
end