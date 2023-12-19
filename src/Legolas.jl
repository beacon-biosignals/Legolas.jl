module Legolas

using Tables, Arrow, UUIDs
using ConstructionBase: ConstructionBase

const LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY = "legolas_schema_qualified"

include("lift.jl")
include("schemas.jl")
include("tables.jl")
include("record_merge.jl")

end # module
