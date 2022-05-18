module Legolas

using Tables, Arrow



const LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY = "legolas_schema_qualified"

include("lift.jl")
include("rows.jl")
include("tables.jl")

end # module
