module Legolas

using Tables, Arrow

const LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY = "legolas_schema_qualified"

include("lift.jl")
include("schemas.jl")
include("tables.jl")

#=
CHANGELOG

- rm'd `Legolas.Row` in favor of `Legolas.row`
- rm'd methods against `::Type{<:Schema}` in favor of `::Schema`
- `Legolas.@row` -> `Legolas.@schema`
- better introspection/validation tools (`schema_fields`, `schema_field_statements`, compliance predicate)
- better enforcement of expected properties:
    - no duplicate fields in schema definitions
    - https://github.com/beacon-biosignals/Legolas.jl/issues/51
    - https://github.com/beacon-biosignals/Legolas.jl/issues/53
- otherwise resolves/obviates:
    - https://github.com/beacon-biosignals/Legolas.jl/issues/49
    - https://github.com/beacon-biosignals/Legolas.jl/issues/50

TODO: deprecations for:

- `Legolas.Row`
- `Legolas.@row`
- relevant methods against `Type{<:Schema}` instead of `Schema`
=#

end # module
