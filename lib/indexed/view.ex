defmodule Indexed.View do
  @moduledoc """
  A data structure about a view, held in ETS.

  While prefilters may be defined statically when warming an index, views
  also define prefilters, but they are tailor-made result sets which can be
  created and destroyed throughout the `t:Indexed.t/0` lifecycle.

  Views define a prefilter to base its result set on. (`nil` is acceptable
  for full result set.) Then, a `:filter` function is used to further narrow
  the results.
  """
  alias __MODULE__

  defstruct [:filter, :maintain_unique, :params, :prefilter]

  @typedoc """
  * `:filter` - A function which takes a record and returns a truthy value if
    it should be included in the result set. Required.
  * `:maintain_unique` - List of field name atoms for which a list of unique
    values under the view will be managed. These lists can be fetched via
    `Indexed.get_uniques_list/4` and `Indexed.get_uniques_map/4`.
  * `:params` - Original user params for building `:filter` and `:prefilter`.
    These are kept mainly for authorization checking in the depending
    application.
  * `:prefilter` - The base prefilter from which the `:filter` further refines.
    `nil` for the full record set.
  """
  @type t :: %View{
          filter: Indexed.filter(),
          maintain_unique: [atom],
          params: keyword,
          prefilter: Indexed.prefilter()
        }

  @typedoc """
  A fingerprint is by taking the relevant parameters which were used to
  construct the prefilter (if applicable) and filter function. This string is
  used to identify the particular view in the map returned by under the key
  named by `Indexed.view_key/1`.
  """
  @type fingerprint :: String.t()

  @typedoc """
  Maps fingerprints to view structs.
  Stored in the index key returned by `Indexed.views_key/1`.
  """
  @type views_map :: %{fingerprint => t}

  @doc """
  Create a unique identifier string for `params`.

  This is not used internally, but is intended as a useful tool for a caller
  for deciding whether to use an existing view or create a new one. It is
  expected that `params` would be used to create the prefilter and/or filter
  function.
  """
  @spec fingerprint(keyword | map) :: String.t()
  def fingerprint(params) do
    string =
      params
      |> Keyword.new()
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn
        {k, v} when is_binary(v) or is_atom(v) -> "#{k}.#{v}"
        {k, v} -> "#{k}.#{inspect(v)}"
      end)
      |> Enum.join(":")

    :sha256
    |> :crypto.hash(string)
    |> Base.encode16()
    |> String.downcase()
    |> String.slice(0, 24)
  end
end
