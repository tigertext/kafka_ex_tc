defmodule KafkaEx.TtConsumer do
  @moduledoc false
  use KafkaEx.GenConsumer
  require Logger

  def handle_message_set(message_set, state) do
    for %Message{value: message} <- message_set do
      Logger.debug(fn -> "message: " <> inspect(message) end)
    end

    {:sync_commit, state}
  end
end
