defmodule Brahmaputra.MixProject do
  use Mix.Project

  def project do
    [
      app: :brahmaputra,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      # Deliberately empty: the driver needs nothing beyond OTP (gzip comes
      # from :zlib), so building it never touches Hex.
      deps: []
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end
end
