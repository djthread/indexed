[
  import_deps: [:ecto],
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  export: [
    locals_without_parens: [managed: 2, managed: 3]
  ]
]
