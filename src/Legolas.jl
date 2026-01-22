module Legolas

using Tables, Arrow, UUIDs

# public API
if VERSION >= v"1.11.0-DEV.469"
    eval(Meta.parse("""
    public SchemaVersion, @schema, @version, @check, is_valid_schema_name,
           parse_identifier, name, version, identifier, schema_provider, parent,
           declared_fields, declaration, record_type, schema_version_from_record,
           declared, find_violation, find_violations, complies_with, validate,
           accepted_field_type, extract_schema_version, write, read, tobuffer,
           lift, construct, record_merge, gather, locations, materialize
    """))
end

const LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY = "legolas_schema_qualified"
const LEGOLAS_SCHEMA_PROVIDER_NAME_METADATA_KEY = "legolas_julia_schema_provider_name"
const LEGOLAS_SCHEMA_PROVIDER_VERSION_METADATA_KEY = "legolas_julia_schema_provider_version"

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
