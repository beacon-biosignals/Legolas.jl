module Legolas

using Tables, Arrow
using Arrow.ArrowTypes

"""
    lift(f, x)

Return `f(x)` unless `x isa Union{Nothing,Missing}`, in which case return `missing`.

This is particularly useful when handling values from `Arrow.Table`, whose null values
may present as either `missing` or `nothing` depending on how the table itself was
originally constructed.
"""
lift(::Any, ::Union{Nothing,Missing}) = missing
lift(f, x) = f(x)

const LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY = "legolas_schema_qualified"

include("rows.jl")
include("tables.jl")

end # module
