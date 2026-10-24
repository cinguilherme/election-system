# {{module}}

{{description}}

## Usage

```clojure
(require '[{{ns}}.core :as core])
```

## Conventions

- `.version` is the single source of truth; `make publish BUMP=minor` bumps it and deploys.
- `docs/` holds the design notes: what was rejected and why.
- `make lint` and `make tests` are the gates; a behaviour change needs a test that fails before
  it and passes after.
