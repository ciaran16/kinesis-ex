defmodule KinesisExTest do
  use ExUnit.Case

  test "fails when trying to read with stream not opened for read" do
    stream = KinesisEx.open("test")
    assert {:error, _} = KinesisEx.read(stream)
  end
end
