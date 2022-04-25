# defmodule Indexed.Managed.ChildSpec do

#   defstruct :cardinality, :name, :id_key, :owner_key
#   @typedoc """
#   An association spec defines an association to another entity.
#   It is used to build the preload function among other things.

#   * `{:one, entity_name, id_key}` - Preload function should get a record of
#     `entity_name` with id matching the id found under `id_key` of the record.
#   * `{:many, entity_name, pf_key, order_hint}` - Preload function should
#     use `Indexed.get_records/4`. If `pf_key` is not null, it will be replaced
#     with `{pfkey, id}` where `id` is the record's id.
#   * `{:repo, key, managed}` - Preload function should use `Repo.get/2` with the
#     assoc's module and the id in the foreign key field for `key` in the record.
#     This is the default when a child/assoc_spec isn't defined for an assoc.
#   """
#   @type assoc_spec ::
#           {:one, entity_name :: atom, id_key :: atom}
#           | {:many, entity_name :: atom, pf_key :: atom | nil, order_hint}
#           | {:repo, assoc_field :: atom, managed :: t}

# end
