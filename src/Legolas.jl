module Legolas

using Tables, Arrow

const LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY = "legolas_schema_qualified"

include("lift.jl")
include("schemas.jl")
include("tables.jl")

# TODO: deprecations for:
#
# - `Legolas.Row`` + `Legolas.@row`
# - relevant methods against `Type{<:Schema}` instead of `Schema`
#

end # module
