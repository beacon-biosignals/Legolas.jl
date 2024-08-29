module Legolas

using Tables, Arrow, UUIDs

const LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY = "legolas_schema_qualified"

include("lift.jl")
include("constraints.jl")
include("schemas.jl")
include("tables.jl")
include("record_merge.jl")

# TODO: Once we require Julia 1.9 or later at a minimum, we can remove this as well as
# all entries in the Project.toml `[deps]` section that are also listed in `[weakdeps]`.
if !isdefined(Base, :get_extension)
    include(joinpath(@__DIR__(), "..", "ext", "LegolasConstructionBaseExt.jl"))
    using .LegolasConstructionBaseExt
end

end # module
