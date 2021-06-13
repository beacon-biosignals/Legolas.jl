# This file provides an introductory tour for Legolas.jl. Run lines in the REPL
# to inspect output at each step! Tests are littered throughout to demonstrate
# functionality in a concrete manner, and so that we can ensure examples stay
# current as the package evolves.

using Legolas, Arrow, Tables, Test

using Legolas: @row, Schema

#####
##### Introduction to the `@row` Macro and `Legolas.Row` constructors
#####
# The most interesting bit of Legolas.jl functionality is the package's `@row` macro, which callers can use
# define new [`Tables.AbstractRow`-compliant](https://tables.juliadata.org/stable/#Tables.AbstractRow-1)
# row types that exhibit some opinionated (but desirable!) properties w.r.t. composability/extensibility.
#
# These row type properties are fundamental to the wider data curation patterns that Legolas.jl seeks to
# facilitate, so let's explore them before we dig deeper into Legolas.jl's other table-centric utilities.

# Declare a `Legolas.Schema` with name `"my-schema"` at version `1` whose *required fields*
# `a`, `b`, `c`, `d`, and `e` are specified via the provided assignment expressions, then return
# a corresponding row type that matches the declared schema:
const MyRow = @row("my-schema@1",
                   a::Real = sin(a),
                   b::String = string(a, b, c),
                   c = [a, b, c],
                   d::Int,
                   e)

# `MyRow` is thus an alias for the type returned by the `@row` macro:
@test MyRow == Legolas.Row{typeof(Schema("my-schema@1"))}

# The `MyRow` type has several useful constructors. Let's start with the constructor that
# accepts all required fields as keyword arguments:
row = MyRow(a=1.5, b="hello", c="goodbye", d=2, e=["anything"])

# By examining `row`'s fields, we can see how the assignment expressions from `MyRow`'s
# `@row` declaration were applied in a simple linear fashion to the input arguments:
@test row.a == sin(1.5)
@test row.b == string(sin(1.5), "hello", "goodbye")
@test row.c == [sin(1.5), string(sin(1.5), "hello", "goodbye"), "goodbye"]
@test row.d == 2
@test row.e == ["anything"]

# In fact, the field assignment expressions provided to the `@row` macro are interpolated
# nearly as-is into the underlying code generated by `@row`. For example, the relevant code
# generated by `MyRow`'s `@row` declaration looks roughly like:
#
#     function Legolas._transform(::typeof(Legolas.Schema("my-schema", 1));
#                                 a=missing, b=missing, c=missing, d=missing, e=missing,
#                                 other...)
#         a::Real = sin(a)
#         b::String = string(a, b, c)
#         c::Any = [a, b, c]
#         d::Int = d
#         e::Any = e
#         return (; a, b, c, d, e, other...)
#     end
#
# This `Legolas._transform` method is invoked at the core of the `MyRow` constructor. As you
# might have noticed, this method has two interesting properties we haven't yet demonstrated.
#
# Here we demonstrate the first property - required fields have a `missing` value by default:
@test isequal(MyRow(a=1.5, b="hello", c="goodbye", d=2), MyRow((; a=1.5, b="hello", c="goodbye", d=2, e=missing)))
@test_throws MethodError MyRow(a=1.5, b="hello", c="goodbye") # correctly throws a `MethodError` when evaluating `d::Int = missing`

# And here's a demonstration of the second property - callers may pass in any other fields in
# addition to the required fields:
row = MyRow(a=1.5, b="hello", c="goodbye", my_other_field=":)", d=2, e=["anything"])
@test row.my_other_field == ":)"

# Finally, there's also a `MyRow` constructor that accepts any `Tables.AbstractRow`-compliant value,
# and extracts all input fields from that value. Here, we demonstrate with a `NamedTuple`:
@test row == MyRow((a=1.5, b="hello", c="goodbye", d=2, e=["anything"], my_other_field=":)"))

# To summarize:
#
# - Inputs to `Legolas.Row` constructors...
#     - ...may be any `Tables.AbstractRow`-compliant value
#     - ...may contain required fields in any order
#     - ...may elide required fields, in which case the constructor will assume them to be `missing`
#     - ...may contain any other fields in addition to the required fields
# - Outputs of `Legolas.Row` constructors...
#     - ...will contain all required fields ("missing" fields are explicitly presented with `missing` values)
#     - ...will order all required fields as specificed by the input `Schema`
#     - ...will contain all given non-required fields after required fields, in the order provided by the caller

#####
##### Extending Existing Rows/Schemas
#####
# Row types declared via `@row` can inherit the fields and transformations specified by other `@row`-declared types.
# Niftily, the properties of `Legolas.Row` that we demonstrated above enable this extension mechanism to be
# implemented via composition under the hood.

# Declare a row type whose schema is named `"my-child-schema"` at version `1` that inherits the fields of the
# `my-schema@1` schema that we defined in the previous section.
const MyChildRow = @row("my-child-schema@1" > "my-schema@1",
                        f::Int = f + 1,
                        g::String,
                        c = last(c))

# The constructor for `MyChildRow` will first apply its parent's transformation before applying its own. The
# effect of this behavior can be seen clearly in the `c` field value in the following example:
input = (a=1.5, b="hello", c="goodbye", d=2, e=["anything"], f=3, g="foo")
child = MyChildRow(input)
@test child.a == sin(1.5)
@test child.b == string(sin(1.5), "hello", "goodbye")
@test child.c == "goodbye"
@test child.d == 2
@test child.e == ["anything"]
@test child.f == 4
@test child.g == "foo"

# A note on syntax: You might ask "Why use `>` as the inheritance operator instead of `<:`?" There are actually three reasons. Firstly,
# `<:` is canonically a subtyping operator that implies the Liskov substition principle, but because Legolas allow arbitrary RHS code in
# required field declarations, a "child" row is not de facto substitutable for its parent. Secondly, `>` implies the actual ordering that
# row transformations are applied in; the parent transformation comes before the child transformation. Thirdly, the child row will usually
# (though technically not always) have a greater total number of required fields than the parent row.

#####
##### Validating/Writing/Reading `Arrow.Table`s with Legolas.jl
#####

# Legolas provides special methods for reading/writing/validating Arrow tables that utilize `Legolas.Schema`s. To
# start exploring these methods, we'll first construct a dummy table using `row::MyRow` from the previous section:
rows = [row,
        Tables.rowmerge(row; b="a different one"),
        Tables.rowmerge(row; e=:anything)]

# We can validate that `rows` is compliant w.r.t. `schema` via `Legolas.validate`, which will throw a descriptive
# error if the `Tables.schema(rows)` mismatches with the required columns/types specified by the `schema`.
schema = Schema("my-schema", 1)
@test schema === Schema("my-schema@1") # `Schema("my-schema", 1)` is an alternative to `Schema("my-schema@1")`
@test (Legolas.validate(rows, schema); true)
invalid = vcat(rows, Tables.rowmerge(row; a="this violates the schema's `a::Real` requirement"))
@test_throws ArgumentError("field `a` has unexpected type; expected <:Real, found Any") Legolas.validate(invalid, schema)

# Legolas also provides `Legolas.write(path_or_io, table, schema; kwargs...)`, which wraps `Arrow.write(path_or_io, table; kwargs...)`
# and performs two additional operations atop the usual operations performed by that function:
#
# - By default, the provided Tables.jl-compliant `table` is validated against `schema` via `Legolas.validate` before
#   it is actually written out. Note that this can be disabled by passing `validate=false` to `Legolas.write`.
#
# - `Legolas.write` ensures that the written-out Arrow table's metadata contains a `"legolas_schema_qualified"`
#   key whose value is `Legolas.schema_qualified_string(schema)`. This field enables consumers of the table to
#   perform automated (or manual) schema discovery/evolution/validation.

schema = Schema("my-child-schema", 1) # For this example, let's use a schema that has a parent
rows = [child,
        Tables.rowmerge(child; b="a different one"),
        Tables.rowmerge(child; e=:anything)]
io = IOBuffer()
Legolas.write(io, rows, schema)
t = Arrow.Table(seekstart(io))

table_isequal(a, b) = isequal(Legolas.materialize(a), Legolas.materialize(b))

@test Arrow.getmetadata(t) == Dict("legolas_schema_qualified" => "my-child-schema@1>my-schema@1")
@test table_isequal(t, Arrow.Table(Arrow.tobuffer(rows)))
@test table_isequal(t, Arrow.Table(Legolas.tobuffer(rows, schema))) # `Legolas.tobuffer` is analogous to `Arrow.tobuffer`

invalid = vcat(rows, Tables.rowmerge(child; a="this violates the schema's `a::Real` requirement"))
@test_throws ArgumentError("field `a` has unexpected type; expected <:Real, found Any") Legolas.tobuffer(invalid, schema)

# Similarly, Legolas provides `Legolas.read(path_or_io)`, which wraps `Arrow.Table(path_or_io)`
# and performs `Legolas.validate` on the resulting `Arrow.Table` before returning it.
@test table_isequal(Legolas.read(Legolas.tobuffer(rows, schema)), t)
@test_throws ArgumentError("`legolas_schema_qualified` field not found in Arrow table metadata") Legolas.read(Arrow.tobuffer(rows))
invalid = Tables.columns(invalid) # ref https://github.com/JuliaData/Arrow.jl/issues/211
Arrow.setmetadata!(invalid, Dict("legolas_schema_qualified" => "my-child-schema@1>my-schema@1"))
@test_throws ArgumentError("field `a` has unexpected type; expected <:Real, found Union{Missing, Float64, String}") Legolas.read(Arrow.tobuffer(invalid))

# A note about one additional benefit of `Legolas.read`/`Legolas.write`: Unlike their Arrow.jl counterparts,
# these functions are relatively agnostic to the types of provided path arguments. Generally, as long as a
# given `path` supports `Base.read(path)::Vector{UInt8}`, `Base.write(path, bytes::Vector{UInt8})`, and
# `mkpath(dirname(path))`, then `path` will work as an argument to `Legolas.read`/`Legolas.write`. At some
# point, we'd like to make similar upstream improvements to Arrow.jl to render its API more path-type-agnostic.

#####
##### Simple Integer Versioning: You Break It, You Bump It
#####
# TODO

#####
##### Tips For Schema Design
#####
# TODO: Cover the following:
#
#   - forward/backward compatibility via allowing `missing` columns when possible
#   - avoid bumping schema versions by handling the deprecation path in the constructor
#   - prefer idempotency in field expressions when possible
#   - prefer Liskov substitutability when possible

#####
##### Miscellaneous Utilities
#####
