defmodule KinesisEx do
  alias KinesisEx.Stream

  @region Application.get_env(:kinesis_ex, :region, "eu-west-1")

  @type t :: %Stream{}

  @type error :: KinesisEx.Requester.request_error

  @doc """
  Opens the Kinesis data stream named `stream_name` for reading and writing. Returns `{:ok, stream}` if successful or `{:error, error}` otherwise.

  Options:
  - `:prepare_read` - set to true to open the stream for reading now, rather than on first read.
  - `:region` - set the region. Defaults to eu-west-1.
  """
  @spec open(String.t(), keyword()) :: {:ok, t} | {:error, error}
  def open(stream_name, opts \\ []) do
    read = Keyword.get(opts, :prepare_read, false)
    region = Keyword.get(opts, :region, @region)
    request_implementation = Keyword.get(opts, :request, KinesisEx.Request.ExAws)
    requester = KinesisEx.Requester.create(request_implementation, stream_name, region)
    Stream.open(requester, read)
  end

  @doc """
  Opens the Kinesis data stream named `stream_name` for reading and writing, and returns the stream. If there is an error, an exception is raised.

  See `open/2` for more details.
  """
  def open!(stream_name, opts \\ []) do
    case open(stream_name, opts) do
      {:ok, stream} -> stream
      {:error, error} -> raise("Error opening #{stream_name}: #{inspect(error)}")
    end
  end

  def read(stream) do
    Stream.read(stream)
  end

  def read!(stream) do
    case read(stream) do
      {:ok, result} -> result
      {:error, error} -> raise("Error reading from #{Stream.name(stream)}: #{inspect(error)}")
    end
  end

  def write(stream, message) do
    Stream.write(stream, message)
  end

  def write!(stream, message) do
    case write(stream, message) do
      :ok -> :ok
      {:error, error} -> raise("Error writing to #{Stream.name(stream)}: #{inspect(error)}")
    end
  end
end
