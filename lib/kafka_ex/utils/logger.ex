defmodule KafkaEx.Utils.Logger do
  @moduledoc false

  @md []

  def error(msg) do
    log(:error, msg)
  end

  def error(format, args) do
    log(:error, format, args)
  end

  def warn(msg) do
    log(:warning, msg)
  end

  def info(msg) do
    log(:info, msg)
  end

  def debug(msg) do
    log(:debug, msg)
  end

  defp log(level, msg) do
    :lager.log(level, @md, msg)
  end

  defp log(level, format, args) do
    :lager.log(level, @md, format, args)
  end
end
