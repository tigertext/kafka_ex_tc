defmodule KafkaEx.Server0P10P1.Test do
  use ExUnit.Case

  @moduletag :server_0_p_10_and_later

  # specific to this server version because we want to test that the api_versions list is exact
  @moduletag :server_0_p_10_p_1

  @tag :api_version
  test "can retrieve api versions" do
    # note this checks against the version of broker we're running in test
    # api_key, max_version, min_version
    api_versions_kafka_0_11_0_1 = [
      [0, 7, 0],
      [1, 11, 0],
      [2, 5, 0],
      [3, 8, 0],
      [4, 2, 0],
      [5, 1, 0],
      [6, 5, 0],
      [7, 2, 0],
      [8, 7, 0],
      [9, 5, 0],
      [10, 2, 0],
      [11, 5, 0],
      [12, 3, 0],
      [13, 2, 0],
      [14, 3, 0],
      [15, 3, 0],
      [16, 2, 0],
      [17, 1, 0],
      [18, 2, 0],
      [19, 3, 0],
      [20, 3, 0],
      [21, 1, 0],
      [22, 1, 0],
      [23, 3, 0],
      [24, 1, 0],
      [25, 1, 0],
      [26, 1, 0],
      [27, 0, 0],
      [28, 2, 0],
      [29, 1, 0],
      [30, 1, 0],
      [31, 1, 0],
      [32, 2, 0],
      [33, 1, 0],
      [34, 1, 0],
      [35, 1, 0],
      [36, 1, 0],
      [37, 1, 0],
      [38, 1, 0],
      [39, 1, 0],
      [40, 1, 0],
      [41, 1, 0],
      [42, 1, 0],
      [43, 0, 0],
      [44, 0, 0]
    ]

    response = KafkaEx.api_versions()

    %KafkaEx.Protocol.ApiVersions.Response{
      api_versions: api_versions,
      error_code: :no_error,
      throttle_time_ms: _
    } = response

    assert api_versions_kafka_0_11_0_1 ==
             api_versions
             |> Enum.map(&[&1.api_key, &1.max_version, &1.min_version])
  end
end
