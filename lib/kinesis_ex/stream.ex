defmodule KinesisEx.Stream do
  @moduledoc false
  
  alias KinesisEx.{Shard, ShardQueue, Requester}

  @add_shards_timeout 60000

  @enforce_keys [:requester]
  defstruct [:requester, :shards]

  def open(requester, read) do
    case Requester.describe(requester) do
      # Check the status is correct.
      {:ok, %{status: status}} when status not in ["ACTIVE", "UPDATING"] ->
        {:error, :wrong_status}

      {:ok, _desc} when not read ->
        {:ok, %__MODULE__{requester: requester}}

      # If the stream is being opened for reading, we need to get the shards.
      {:ok, _desc} when read ->
        with {:ok, shards} <- shards(requester) do
          {:ok, %__MODULE__{requester: requester, shards: shards}}
        end
    end
  end

  defp shards(requester) do
    with {:ok, shard_ids} <- Requester.shard_ids(requester) do
      # Create each new shard in its own task, since each performs requests for the iterator.
      opts = [timeout: @add_shards_timeout, on_timeout: :kill_task]
      task_stream = Task.async_stream(shard_ids, &Shard.create(&1, requester), opts)

      # Get the results as a list of tuples of the form {:ok, shard} or {:error, error}
      results =
        Enum.map(task_stream, fn
          {:ok, result} -> result
          {:exit, :timeout} -> {:error, :timeout}
        end)

      case Enum.filter(results, &(elem(&1, 1) == :error)) do
        [] -> {:ok, Enum.map(results, fn {:ok, shards} -> shards end) |> ShardQueue.create()}
        [error | _] -> error
      end
    end
  end

  def read(%__MODULE__{requester: requester, shards: nil} = stream) do
    with {:ok, shards} <- shards(requester) do
      stream = %{stream | shards: shards}
      read(stream)
    end
  end

  def read(%__MODULE__{requester: requester, shards: shards} = stream) do
    shard = ShardQueue.front(shards)
    shard_iterator = Shard.iterator(shard)

    case Requester.records(requester, shard_iterator) do
      {:ok, %{next_iterator: next, records: records, last_sequence_number: sequence_number}} ->
        shard = %{shard | iterator: next || shard_iterator, sequence_number: sequence_number}
        stream = %{stream | shards: ShardQueue.update(shards, shard)}
        messages = Enum.map(records, fn %{message: message} -> message end)
        {:ok, {messages, stream}}

      # If the iterator has expired, get a new iterator and retry.
      {:error, :iterator_expired} ->
        with {:ok, shards} <- ShardQueue.refresh_front(shards, requester) do
          read(%{stream | shards: shards})
        end

      {:error, _} = error ->
        error
    end
  end

  def write(%__MODULE__{requester: requester}, message) do
    with {:ok, _resp} <- Requester.put_message(requester, message) do
      :ok
    end
  end

  def name(%__MODULE__{requester: requester}) do
    requester.stream_name
  end
end
