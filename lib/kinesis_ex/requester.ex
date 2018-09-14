defmodule KinesisEx.Requester do
  @moduledoc false

  @type request_error ::
          String.t() | :throughput_or_limit_exceeded | :service_unavailable | :internal_failure

  @type description :: %{status: String.t()}

  @callback describe(stream_name :: String.t(), region :: String.t()) ::
              {:ok, description} | {:error, request_error}

  @callback shard_ids(stream_name :: String.t(), region :: String.t()) ::
              {:ok, [String.t()]} | {:error, request_error}

  @type shard_iterator_type ::
          :oldest
          | :latest
          | {:at_sequence_number, String.t()}
          | {:after_sequence_number, String.t()}

  @callback shard_iterator(
              stream_name :: String.t(),
              region :: String.t(),
              shard_id :: String.t(),
              iterator_type :: shard_iterator_type
            ) :: {:ok, String.t()} | {:error, request_error}

  # `message` will already be decoded.
  @type record :: %{message: String.t()}

  # `last_sequence_number` will be nil when there are no records.
  @type records_info :: %{
          millis_behind: integer,
          next_iterator: String.t(),
          records: [record],
          last_sequence_number: String.t() | nil
        }

  @callback records(stream_name :: String.t(), region :: String.t(), iterator :: String.t()) ::
              {:ok, records_info} | {:error, request_error | :iterator_expired}

  @callback put_message(stream_name :: String.t(), region :: String.t(), message :: String.t()) ::
              :ok | {:error, request_error}

  # Dynamic dispatch.

  @max_attempts 10

  @retryable_errors [:throughput_or_limit_exceeded, :service_unavailable, :internal_failure]

  @enforce_keys [:module, :stream_name, :region]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          module: atom,
          stream_name: String.t(),
          region: String.t()
        }

  @spec create(atom, String.t(), String.t()) :: t
  def create(implementation, stream_name, region) do
    %__MODULE__{module: implementation, stream_name: stream_name, region: region}
  end

  @spec describe(t) :: {:ok, description} | {:error, request_error}
  def describe(requester) do
    perform(requester, :describe)
  end

  @spec shard_ids(t) :: {:ok, [String.t()]} | {:error, request_error}
  def shard_ids(requester) do
    perform(requester, :shard_ids)
  end

  @spec shard_iterator(t, String.t(), shard_iterator_type) ::
          {:ok, String.t()} | {:error, request_error}
  def shard_iterator(requester, shard_id, shard_iterator_type) do
    perform(requester, :shard_iterator, [shard_id, shard_iterator_type])
  end

  def records(requester, shard_iterator) do
    perform(requester, :records, [shard_iterator])
  end

  def put_message(requester, message) do
    perform(requester, :put_message, [message])
  end

  defp perform(
         %__MODULE__{module: module, stream_name: stream_name, region: region} = requester,
         operation,
         args \\ [],
         attempt \\ 0
       ) do
    require Logger

    case apply(module, operation, [stream_name, region | args]) do
      {:error, error} when error in @retryable_errors and attempt < @max_attempts ->
        # TODO improve backoff.
        Logger.warn("Retrying error #{inspect(error)}.")
        Process.sleep(3000)
        perform(requester, operation, args, attempt + 1)

      result ->
        result
    end
  end
end
