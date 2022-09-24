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
- better introspection/validation tools (`schema_fields`, `schema_field_statements`, compliance predicates, etc.)
- expected properties of new schema definitions are now better validated/enforced:
    - schema declarations should only contain one statement per required field is now enforced
    - https://github.com/beacon-biosignals/Legolas.jl/issues/51
    - https://github.com/beacon-biosignals/Legolas.jl/issues/53
- Previously, it was technically possible to alter a preexisting schema declaration, and for such changes to be
automatically reflected in the behavior of that schema's children within the same Julia session. For example, you
could redeclare a preexisting schema `foo@1` with an additional required field compared to its original declaration,
which would de facto cause `Legolas.validate` to require the field for `foo@1`'s child schemas. Since there was little
enforcement of actual parent/child field compatibility in the first place, it was possible to redefine schemas in such a
manner that rendered them incompatible with their preexisting children. Now that we seek to better enforce parent/child
field compatibility, we should strive to disallow this kind of accidental invalidation. We achieve this by bluntly disallowing
the alteration of preexisting schema declarations via redeclaration entirely, because a) there's not currently a mechanism for
validating parent schema declarations against preexisting child schema declarations and b) the ability to alter preexisting
schema declarations is of questionable utility in the first place.
- otherwise resolves/obviates:
    - https://github.com/beacon-biosignals/Legolas.jl/issues/49
    - https://github.com/beacon-biosignals/Legolas.jl/issues/50
    - https://github.com/beacon-biosignals/Legolas.jl/issues/44
    - https://github.com/beacon-biosignals/Legolas.jl/issues/41

TODO: deprecations for:

- `Legolas.Row`
- `Legolas.@row`
- relevant methods against `Type{<:Schema}` instead of `Schema`
=#

end # module
