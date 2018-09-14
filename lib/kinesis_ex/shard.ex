defmodule KinesisEx.Shard do
  @moduledoc false
  
  alias KinesisEx.Requester

  @enforce_keys [:id, :iterator]
  defstruct [:id, :iterator, :sequence_number]

  def create(id, requester, sequence_number \\ nil) do
    type = if sequence_number, do: {:at_sequence_number, sequence_number}, else: :oldest

    with {:ok, iterator} <- Requester.shard_iterator(requester, id, type) do
      {:ok, %__MODULE__{id: id, iterator: iterator, sequence_number: sequence_number}}
    end
  end

  def refresh(%__MODULE__{id: id, sequence_number: sequence_number}, requester) do
    create(id, requester, sequence_number)
  end

  def iterator(%__MODULE__{iterator: iterator}) do
    iterator
  end
end
