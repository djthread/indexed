defmodule Indexed.Managed.Prepare do
  @moduledoc """
  Some tools for preparation and data normalization.
  """
  alias Indexed.{Entity, Managed}

  @doc """
  Make some automatic adjustments to the manageds list:

  * Set the `:tracked` option on the managed structs where another references it
    with a `:one` association.
  * More things.
  """
  @spec rewrite_manageds([Managed.t()]) :: [Managed.t()]
  def rewrite_manageds(manageds) do
    put_fn = fn k, fun -> &%{&1 | k => fun.(&1)} end

    map_put = fn mgs, k, fun ->
      Enum.map(mgs, put_fn.(k, &fun.(&1, mgs)))
    end

    manageds
    |> map_put.(:children, &do_rewrite_children/2)
    |> map_put.(:prefilters, &do_rewrite_prefilters/2)
    |> map_put.(:fields, &do_rewrite_fields/2)
    |> map_put.(:tracked, &do_rewrite_tracked/2)
  end

  # Normalize child association specs. Takes managed to update and list of all.
  @spec do_rewrite_children(Managed.t(), [Managed.t()]) :: Managed.children()
  defp do_rewrite_children(%{children: children, module: mod}, manageds) do
    spec = &child_spec_from_ecto(mod, &1, manageds)

    Map.new(children, fn
      k when is_atom(k) ->
        {k, spec.(k)}

      {k, spec} when :many == elem(spec, 0) ->
        {k, normalize_spec(spec)}

      {k, order_by: order_by} ->
        {k, with({:many, a, b, nil} <- spec.(k), do: {:many, a, b, order_by})}

      other ->
        other
    end)
  end

  defp child_spec_from_ecto(mod, field, manageds) do
    case mod.__schema__(:association, field) do
      %{cardinality: :one} = a ->
        {:one, entity_by_module(manageds, a.related), a.owner_key}

      %{cardinality: :many} = a ->
        {:many, entity_by_module(manageds, a.related), a.related_key, nil}

      nil ->
        raise "#{inspect(mod)} lacks ecto association #{field}."
    end
  end

  # Auto-add prefilters needed for foreign many assocs to operate.
  # For instance, :comments would need :post_id prefilter
  # because :posts has :many comments.
  @spec do_rewrite_prefilters(Managed.t(), [Managed.t()]) :: [atom | Entity.prefilter_config()]
  defp do_rewrite_prefilters(%{prefilters: prefilters, name: name}, manageds) do
    finish = fn required ->
      Enum.uniq_by(
        Indexed.Actions.Warm.resolve_prefilters_opt(required ++ prefilters),
        &elem(&1, 0)
      )
    end

    required =
      Enum.reduce(manageds, [], fn %{children: children}, acc ->
        Enum.reduce(children, [], fn
          {_k, {:many, ^name, pf_key, _}}, acc2 -> [pf_key | acc2]
          _, acc2 -> acc2
        end) ++ acc
      end)

    if Enum.empty?(required),
      do: prefilters,
      else: finish.(required)
  end

  # If :fields is empty, use the id key or the first field given by Ecto.
  @spec do_rewrite_fields(Managed.t(), [Managed.t()]) :: [atom | Entity.field()]
  defp do_rewrite_fields(%{fields: [], id_key: id_key}, _) when is_atom(id_key),
    do: [id_key]

  defp do_rewrite_fields(%{fields: [], module: mod}, _),
    do: [hd(mod.__schema__(:fields))]

  defp do_rewrite_fields(%{fields: fields}, _), do: fields

  # Return true for tracked if another entity has a :one association to us.
  @spec do_rewrite_tracked(Managed.t(), [Managed.t()]) :: boolean
  defp do_rewrite_tracked(%{name: name}, manageds) do
    Enum.any?(manageds, fn m ->
      Enum.any?(m.children, &match?({:one, ^name, _}, elem(&1, 1)))
    end)
  end

  # Find the entity name in manageds using the given schema module.
  @spec entity_by_module([Managed.t()], module) :: atom
  defp entity_by_module(manageds, mod) do
    Enum.find_value(manageds, fn
      %{name: name, module: ^mod} -> name
      _ -> nil
    end) || raise "No entity module #{mod} in #{inspect(Enum.map(manageds, & &1.module))}"
  end

  @spec validate_before_compile!(module, module, list) :: :ok
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  def validate_before_compile!(mod, _repo, managed) do
    for %{children: _children, module: module, name: name, subscribe: sub, unsubscribe: unsub} <-
          managed do
      inf = "in #{inspect(mod)} for #{name}"

      if (sub != nil and is_nil(unsub)) or (unsub != nil and is_nil(sub)),
        do: raise("Must have both :subscribe and :unsubscribe or neither #{inf}.")

      function_exported?(module, :__schema__, 1) ||
        raise "#{inspect(module)} should be an Ecto.Schema module #{inf}"
    end

    :ok
  end

  # Many tuple: {:many, entity_name, prefilter_key, order_hint}
  # Optional: prefilter_key, order_hint
  @spec normalize_spec(tuple) :: tuple
  defp normalize_spec(tup) when :many == elem(tup, 0), do: expand_tuple(tup, 4)
  defp normalize_spec(tup), do: tup

  # Pad `tuple` up to `num` with `nil`.
  @spec expand_tuple(tuple, non_neg_integer) :: tuple
  defp expand_tuple(tuple, num) do
    case length(Tuple.to_list(tuple)) do
      len when len >= num ->
        tuple

      len ->
        Enum.reduce((len + 1)..num, tuple, fn _, acc ->
          Tuple.append(acc, nil)
        end)
    end
  end
end
