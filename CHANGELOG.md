# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic
Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - 2023-02-03

### Changed
- Nothing. No changes. Updating only to debug a potential release issue.

## [0.3.0] - 2023-02-01

### Added
- `Indexed.delete_tables/1` to delete all ETS tables.
  Useful when indexing from scratch is needed.
- `MyManagedMod.{get_ids_by, get_lookup}`

### Changed
- `Indexed.get_index/4` (and `Indexed.Managed.get_index/4`) now always
  return a list. (They used to return nil if there was no index.)

### Removed

- `Indexed.lookup` - Use `Indexed.get_by/4` instead.

## [0.2.0] - 2022-12-16

### Added
- Managed: `:repo` option on `use Indexed.Managed` is now optional. Also,
  specifying a module on the `managed` lines is optional. This means that
  Managed can now work with non-`Ecto.Schema` maps, just like when using
  Indexed directly. Some auto-discovery features may not be available, though.
- Prewarm ability on Indexed and Managed layers. This means a
  `c:GenServer.init/1` can validate and initialize configuration, then create
  the ETS tables, without inserting data. A `c:GenServer.handle_continue/2`
  can then be used to do the heavy work of loading the data.

### Changed
- Managed: `field` sort option is now `:datetime` instead of `:date_time`.


## [0.1.0] - 2022-11-30

### Added
- Namespace mode: If an atom is passed to `use Indexed.Managed` in the
  `:namespace` option, then ETS tables for this instance of indexed will use
  named tables. This means that other processes can access the data directly as
  long as they are on the same node. Getter functions will be attached to the
  module which do not require any state.
- Lookups: An entity can now be configured (via Managed or Indexed directly)
  with one or more fields under the `:lookups` option. Lookup maps will be
  auto-maintained for these fields such that `Indexed.get_by/4` can look up a
  list of IDs of records carrying a given value.

### Changed
- Properly exporting locals_without_parens in `.formatter.exs` so `managed`
  macro can be used without parens.
- `Indexed.get_records/4` and `Indexed.get_uniques_list/4` now return an empty
  list instead of `nil`. `Indexed.get_uniques_map/4` now returns an empty map
  instead of `nil`.
- Managed: Top-level keys in the `:managed_path` option will be auto-attached to
  the `:children` option.

### Fixed
- Fixes around Paginator being an optional dependency.


## [0.0.1] - 2022-04-25

### Added

- Initial release
