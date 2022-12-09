# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic
Versioning](https://semver.org/spec/v2.0.0.html).


## [0.2.0] - Unreleased

### Added
- Managed: `:repo` option on `use Indexed.Managed` is now optional. Also,
  specifying a module on the `managed` lines is optional. This means that
  Managed can now work with non-`Ecto.Schema` maps, just like when using
  Indexed directly. Some auto-discovery features may not be available, though.

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
