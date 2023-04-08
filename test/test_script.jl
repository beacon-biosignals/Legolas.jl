using Pkg
Pkg.activate(".")

using TestEnv; TestEnv.activate()
# include("runtests.jl")

# t = Tables.Schema((:a, :y, :z), (Int32, String, Any))
# s = GrandchildV1SchemaVersion()

# Legolas.find_violation(t, s)
# @info Legolas.find_violations(t, s)

# using Legolas: required_fields
# s = GrandchildV1SchemaVersion()
# schema_version = s
