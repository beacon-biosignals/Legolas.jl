module Legolas

using Tables, Arrow, UUIDs, ConstructionBase

const LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY = "legolas_schema_qualified"

include("lift.jl")
include("schemas.jl")
include("tables.jl")

end # module
