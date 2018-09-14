defmodule KinesisEx.Request.ExAws do
  @moduledoc false
  
  @behaviour KinesisEx.Requester

  def describe(stream_name, region) do
    case ExAws.Kinesis.describe_stream(stream_name, limit: 1) |> ExAws.request(region: region) do
      {:ok, resp} ->
        desc = resp["StreamDescription"]
        desc = %{status: desc["StreamStatus"]}
        {:ok, desc}

      {:error, error} ->
        {:error, transform_error(error)}
    end
  end

  def shard_ids(stream_name, region) do
    case ExAws.Kinesis.describe_stream(stream_name) |> ExAws.request(region: region) do
      {:ok, resp} ->
        # TODO support more than 100 shards.
        if resp["StreamDescription"]["HasMoreShards"] do
          raise("Currently only 100 shards are supported")
        end

        {:ok, get_in(resp, ["StreamDescription", "Shards", Access.all(), "ShardId"])}

      {:error, error} ->
        {:error, transform_error(error)}
    end
  end

  def shard_iterator(stream_name, region, shard_id, iterator_type) do
    {ex_aws_iterator_type, opts} =
      case iterator_type do
        :oldest -> {:trim_horizon, []}
        :latest -> {:latest, []}
        {type, sequence_number} -> {type, [starting_sequence_number: sequence_number]}
      end

    op = ExAws.Kinesis.get_shard_iterator(stream_name, shard_id, ex_aws_iterator_type, opts)

    case ExAws.request(op, region: region) do
      {:ok, resp} -> {:ok, Map.fetch!(resp, "ShardIterator")}
      {:error, error} -> {:error, transform_error(error)}
    end
  end

  def records(_stream_name, region, iterator) do
    case ExAws.Kinesis.get_records(iterator) |> ExAws.request(region: region) do
      {:ok, resp} ->
        records_unformatted = resp["Records"]
        records = Enum.map(records_unformatted, &%{message: Base.decode64!(&1["Data"])})

        info = %{
          millis_behind: Map.fetch!(resp, "MillisBehindLatest"),
          next_iterator: Map.fetch!(resp, "NextShardIterator"),
          records: records,
          # This will be nil if the list is empty.
          last_sequence_number: List.last(records_unformatted)["SequenceNumber"]
        }

        {:ok, info}

      {:error, {:http_error, 400, %{"__type" => "ExpiredIteratorException"}}} ->
        {:error, :iterator_expired}

      {:error, error} ->
        {:error, transform_error(error)}
    end
  end

  def put_message(stream_name, region, message) do
    # The random number just decides which shard the record will be placed in.
    r = Enum.random(1..1_000_000) |> Integer.to_string()

    case ExAws.Kinesis.put_record(stream_name, r, message) |> ExAws.request(region: region) do
      {:ok, _resp} -> :ok
      {:error, error} -> {:error, transform_error(error)}
    end
  end

  defp transform_error(
         {:http_error, 400, %{"__type" => "ProvisionedThroughputExceededException"}}
       ) do
    :throughput_or_limit_exceeded
  end

  defp transform_error({:http_error, 400, %{"__type" => "LimitExceededException"}}) do
    :throughput_or_limit_exceeded
  end

  defp transform_error({:http_error, 503, _}) do
    :service_unavailable
  end

  defp transform_error({:http_error, 500, _}) do
    :internal_failure
  end

  defp transform_error(error) do
    inspect(error)
  end
end
