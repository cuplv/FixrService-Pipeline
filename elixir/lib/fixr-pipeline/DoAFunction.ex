defmodule Fixr-Pipeline.DoAFunction do
    def function do
        receive do
            {aToB, a, sender} ->
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

    #def program do
    #    receive do
    #        {path, args, sender} ->
    #            System.cmd(path, args)
    #            {:ok, :yourenotdonewiththisyet}
    #    end
    #end
end