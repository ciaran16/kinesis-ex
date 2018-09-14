defmodule KinesisEx.ShardQueue do
  @moduledoc false

  alias KinesisEx.Shard

  @type t :: :queue.queue(Shard)

  @doc """
  Creates a shard queue from a non-empty list of shards.
  """
  def create([%Shard{} | _] = shards) do
    :queue.from_list(shards)
  end

  @doc """
  Returns the shard at the front of the queue.
  """
  def front(shards) do
    :queue.get(shards)
  end

  @doc """
  Updates the shard at the front of the queue by removing it and adding the updated shard `shard` into the queue. This keeps the queue at the same length.
  """
  def update(shards, shard) do
    shards = :queue.drop(shards)
    :queue.in(shard, shards)
  end

  @doc """
  Gets a new shard iterator for shard at the front of the queue. Returns the updated queue.
  """
  def refresh_front(shards, requester) do
    with {:ok, shard} <- front(shards) |> Shard.refresh(requester) do
      shards = :queue.drop(shards)
      {:ok, :queue.in_r(shard, shards)}
    end
  end
end
