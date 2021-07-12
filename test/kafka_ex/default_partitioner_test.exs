defmodule KafkaEx.DefaultPartitionerTest do
  alias KafkaEx.DefaultPartitioner

  alias KafkaEx.Protocol.Produce.Request, as: ProduceRequest
  alias KafkaEx.Protocol.Produce.Message, as: ProduceMessage
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.Metadata.TopicMetadata
  alias KafkaEx.Protocol.Metadata.PartitionMetadata

  use ExUnit.Case

  def metadata(partitions \\ 5) do
    %MetadataResponse{
      topic_metadatas: [
        %TopicMetadata{
          topic: "test_topic",
          partition_metadatas:
            Enum.map(0..(partitions - 1), fn n ->
              %PartitionMetadata{
                partition_id: n
              }
            end)
        }
      ]
    }
  end

  test "no assignment" do
    request = %ProduceRequest{
      topic: "test_topic",
      partition: 2,
      messages: [
        %ProduceMessage{key: nil, value: "message"}
      ]
    }

    %{partition: 2} = DefaultPartitioner.assign_partition(request, metadata(5))
  end

  test "random assignment" do
    request = %ProduceRequest{
      topic: "test_topic",
      partition: nil,
      messages: [
        %ProduceMessage{key: nil, value: "message"}
      ]
    }

    %{partition: partition} =
      DefaultPartitioner.assign_partition(request, metadata(5))

    assert partition >= 0 and partition < 5
  end

  test "key based assignment" do
    request = %ProduceRequest{
      topic: "test_topic",
      partition: nil,
      messages: [
        %ProduceMessage{key: "key", value: "message"}
      ]
    }

    %{partition: 1} = DefaultPartitioner.assign_partition(request, metadata(5))
    %{partition: 1} = DefaultPartitioner.assign_partition(request, metadata(6))

    second_request = %ProduceRequest{
      topic: "test_topic",
      partition: nil,
      messages: [
        %ProduceMessage{key: "key2", value: "message"}
      ]
    }

    %{partition: 1} =
      DefaultPartitioner.assign_partition(second_request, metadata(5))

    %{partition: 5} =
      DefaultPartitioner.assign_partition(second_request, metadata(6))
  end

  test "produce request with inconsistent keys" do
    request = %ProduceRequest{
      topic: "test_topic",
      partition: nil,
      messages: [
        %ProduceMessage{key: "key-1", value: "message-1"},
        %ProduceMessage{key: "key-2", value: "message-2"}
      ]
    }

    #    :dbg.stop_clear
    #    :dbg.start
    #    :dbg.tracer(:process, {
    #      fn {:trace, _, :call,
    #           {KafkaEx.Utils.Logger, :warn,
    #             ["Elixir.KafkaEx.DefaultPartitioner: couldn't assign partition due to :inconsistent_keys"]}}, _ ->
    #        IO.inspect(:okkkkk)
    #      end, 0})
    #
    #    :dbg.tpl(KafkaEx.Utils.Logger, :warn, 1, [])
    #
    #    :dbg.p(:all, :c)
    #
    #    DefaultPartitioner.assign_partition(request, m.(5))
    #
    #
    #    :dbg.stop_clear
    #
    #
    #

    expected_msg =
      "Elixir.KafkaEx.DefaultPartitioner: 1couldn't assign partition due to :inconsistent_keys"

    fun = fn -> DefaultPartitioner.assign_partition(request, metadata(5)) end
    my_capture_log(:warn, 1, fun, expected_msg)

    #    assert capture_log(fn ->
    #             DefaultPartitioner.assign_partition(request, metadata(5))
    #           end) =~
    #             "KafkaEx.DefaultPartitioner: couldn't assign partition due to :inconsistent_keys"
  end

  def my_capture_log(level, arity, fun, expected_msg) do
    :dbg.stop_clear()
    :dbg.start()

    :dbg.tracer(:process, {
      fn
        {:trace, _, :call, {KafkaEx.Utils.Logger, ^level, [^expected_msg]}},
        _ ->
          :ok

        _, _ ->
          :a = :b
      end,
      0
    })

    :dbg.tpl(KafkaEx.Utils.Logger, level, arity, [])
    :dbg.p(:all, :c)
    fun.()
    :dbg.stop_clear()
  end
end
