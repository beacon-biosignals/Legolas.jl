# This file provides an introductory tour for Legolas.jl. Run lines in the REPL
# to inspect output at each step! Tests are littered throughout to demonstrate
# functionality in a concrete manner, and so that we can ensure examples stay
# current as the package evolves.

using Legolas, Arrow, Tables, Test

using Legolas: @schema, Schema, @alias, complies_with, find_violation, validate, row

#####
##### Introduction
#####

# Legolas provides mechanisms for constructing, reading, writing, and validating
# Tables.jl-compliant values against extensible, versioned, user-specified *schemas*.

# We'll dive into the extensibility and versioning aspects of Legolas later. For now,
# let's start the tour by declaring a new Legolas schema via the `@schema` macro.

# Each Legolas schema declaration must specify the declared schema's name, version,
# and the set of required fields that a given row value must contain in order to comply
# with that schema. Let's use the `@schema` macro to declare a new schema named
# `example.foo` at an initial version `1` with the given required fields:
@schema("example.foo@1",
        a::Real,
        b::String,
        c,
        d::AbstractVector)

# This `@schema` declaration automatically overloaded a bunch of methods with respect to
# a special type corresponding to the `example.foo@1` schema. We can create new instances
# of this type via `Legolas.Schema`:
@test Schema("example.foo@1") isa Schema{Symbol("example.foo"),1}
@test Schema("example.foo@1") == Schema("example.foo", 1)

# We can use `@alias` to declare a type alias `Foo{v}` that can be used to more succinctly refer
# to `Schema{Symbol("example.foo"),1}` and construct instances of the `example.foo@1` schema:
@alias("example.foo", Foo)
@test Foo{1} == Schema{Symbol("example.foo"),1}
@test Foo{1}() == Schema("example.foo", 1)

#####
##### Schema Compliance/Validation
#####

# We can use `complies_with`, `validate`, and `find_violation` to check whether a given
# `Tables.Schema` (ref https://tables.juliadata.org/stable/#Tables.Schema) complies with
# our `example.foo@1` schema.

# For example, all of the following `Tables.Schema`s comply with `example.foo@1`:
for s in [Tables.Schema((:a, :b, :c, :d), (Real, String, Any, AbstractVector)), # All required fields must be present...
          Tables.Schema((:a, :b, :c, :d), (Int, String, Float64, Vector)),      # ...and have subtypes that match the schema's declared type constraints.
          Tables.Schema((:b, :a, :d, :c), (String, Int, Vector, Float64)),      # Fields do not have to be in any particular order, as long as they are present.
          Tables.Schema((:a, :b, :d), (Int, String, Vector)),                   # Fields whose declared type constraints are `>:Missing` may be elided entirely.
          Tables.Schema((:a, :x, :b, :y, :d), (Int, Any, String, Any, Vector))] # Non-required fields may also be present.
    # if `complies_with` finds a violation, it returns `false`; returns `true` otherwise
    @test complies_with(s, Foo{1}())

    # if `validate` finds a violation, it throws an error indicating the violation;
    # returns `nothing` otherwise
    @test validate(s, Foo{1}()) isa Nothing

    # if `find_violation` finds a violation, it returns a tuple indicating the relevant
    # field and its violation; returns `nothing` otherwise
    @test isnothing(find_violation(s, Foo{1}()))
end

# ...while the below `Tables.Schema`s do not:

s = Tables.Schema((:a, :c, :d), (Int, Float64, Vector)) # The required non-`>:Missing` field `b::String` is not present.
@test !complies_with(s, Foo{1}())
@test_throws ArgumentError validate(s, Foo{1}())
@test isequal(find_violation(s, Foo{1}()), :b => missing)

s = Tables.Schema((:a, :b, :c, :d), (Int, String, Float64, Any)) # The type of required field `d::AbstractVector` is not `<:AbstractVector`.
@test !complies_with(s, Foo{1}())
@test_throws ArgumentError validate(s, Foo{1}())
@test isequal(find_violation(s, Foo{1}()), :d => Any)

# The expectations that characterize Legolas' particular notion of "schematic compliance" - requiring the
# presence of pre-specified declared fields, assuming non-present fields to be implicitly `missing`, and allowing
# the presence of non-required fields - were chosen such that the question "Does the table `t` comply with the Legolas
# schema `s`?" is roughly equivalent to "Can a logical view be trivially constructed atop table `t` that contains only
# the required fields declared by `s`?" The ability to cleanly ask this question enables a weak notion of "subtyping"
# (see https://en.wikipedia.org/wiki/Duck_typing, https://en.wikipedia.org/wiki/Liskov_substitution_principle) that is
# core to Legolas' mechanisms for defining, extending, and versioning interfaces to tabular data.

#####
##### `Legolas.row`
#####

# `Legolas.row` returns a "canonicalized" schema-compliant row value from a given set of fields,
# provided as either keyword arguments or as an `Tables.AbstractRow`-compliant value:
fields = (a=1.0, b="hi", c=π, d=[1, 2, 3])
@test row(Foo{1}(); fields...) == fields
@test row(Foo{1}(), fields) == fields

# This may seem like a fairly trivial function in the preceding example, but the `row` function has
# some useful and convenient properties. Specifically, input fields provided to `row` may:
#
# - ...contain required fields in any order
# - ...elide required fields, in which case the constructor will assume them to be `missing`
# - ...contain any other fields in addition to the required fields
#
# ...while the output of `row` will always:
#
# - ...be a `Tables.AbstractRow`-compliant value
# - ...comply with the provided schema (otherwise an exception will be thrown)
# - ...contain all required fields (missing required fields are explicitly given `missing` values)
# - ...contain all provided non-required fields
#
# Demonstrating a few of these properties:

# Providing the additional non-required field `x` in the input, which is preserved in the output:
@test row(Foo{1}(); fields..., x="x") == (; fields..., x="x")

# Eliding the required field `c`, which is assigned `missing` in the output:
@test isequal(row(Foo{1}(); a=1.0, b="hi", d=[1, 2, 3]), (a=1.0, b="hi", c=missing, d=[1, 2, 3]))

# Providing the non-compliantly-typed field `d::Int`, inducing a `MethodError`:
@test_throws MethodError row(Foo{1}(); a=1.0, b="hi", d=2)

# Implicitly providing the non-compliantly-typed field `d::Missing`, inducing a `MethodError`:
@test_throws MethodError row(Foo{1}(); a=1.0, b="hi")

#####
##### Custom Field Assignments
#####

# Schema authors may tailor the behavior of `row` for their schema by defining custom field
# assignments in the schema's declaration. The `example.foo@1` schema declaration doesn't feature
# any such assignments, so let's declare a new schema `example.bar@1` that does:
@schema("example.bar@1",
        x::Union{Int8,Missing} = ismissing(x) ? x : clamp(x, Int8),
        y::String = string(y))
@alias("example.bar", Bar)

# The `row` function will then execute these custom field statements every time `row(::Bar{1}; ...)`
# is invoked. If helpful, you can imagine that the `row` method definition generated for `Bar{1}`
# is roughly equivalent to:
#
#   function Legolas.row(::Bar{1}; x=missing, y=missing, other...)
#       x::Union{Int8,Missing} = ismissing(x) ? x : clamp(x, Int8),
#       y::String = string(y)
#       return (; x, y, other...)
#   end
#
# Demonstrating that the behavior in practice aligns with the above description:
@test row(Bar{1}(); x=200, y=:hi) == (x=127, y="hi")
@test isequal(row(Bar{1}(); y=:hi), (x=missing, y="hi"))

# Custom field assignments enable schema authors to enforce value-level constraints and to imbue
# `row` with convenient per-field transformations/conversions so that it can accept a wider range
# of applicable inputs for each field. However, schema authors that use custom field assignments
# must always take care to **preserve idempotency and avoid side effects / reliance on non-local
# state**.
#
# In other words, the following equivalences should always hold for non-pathological schemas:
#
#   row(schema, fields) == row(schema, fields)
#   row(schema, row(schema, fields)) == row(schema, fields)
#
# These are two very important expectations that must be met for `row` to behave as intended,
# but they are not enforceable by Legolas itself, since custom field assignments allow arbitrary
# Julia code; thus, schema authors are responsible for not violating these expectations.
#
# For illustration's sake, here is an example of a pathological schema declaration that violates
# both of these expectations:

const GLOBAL_STATE = Ref(0)

@schema("example.bad@1",
        x::Int = x + 1,
        y = (GLOBAL_STATE[] += y; GLOBAL_STATE[]))
@alias("example.bad", Bad)

# Demonstration of non-idempotency, both in `x` and `y` fields:
@test row(Bad{1}(), row(Bad{1}(), (x=1, y=1))) != row(Bad{1}(), (x=1, y=1))

# Demonstration of side effects / reliance on non-local state in `y` field:
@test row(Bad{1}(), (x=1, y=1)) != row(Bad{1}(), (x=1, y=1))

#####
##### Extending Existing Schemas
#####

#=
# Schemas declared via `@schema` can inherit the fields and transformations specified by other schema declarations.
# Niftily, the properties of schema application that we demonstrated above enable this extension mechanism to be
# implemented via composition under the hood.

# Declare a row type whose schema is named `"my-child-schema"` at version `1` that inherits the fields of the
# `my-schema@1` schema that we defined in the previous section.
@schema("my-child-schema@1" > "my-schema@1",
        d, # "declaring" the underlying `my-schema@1` field here so that it
           # can be referenced in our definition for the `f` field.
        f::Int = f + d,
        g::String,
        c = last(c))

# The constructor for `MyChildRow` will first apply its parent's transformation before applying its own. The
# effect of this behavior can be seen clearly in the `c` field value in the following example:
input = (a=1.5, b="hello", c="goodbye", d=2, e=["anything"], f=3, g="foo")
child = apply(MyChildSchema{1}(), input)
@test child.a == sin(1.5)
@test child.b == string(sin(1.5), "hello", "goodbye")
@test child.c == "goodbye"
@test child.d == 2
@test child.e == ["anything"]
@test child.f == 5
@test child.g == "foo"

# Note that even though we didn't write down any constraints on `d` in our `my-child-schema@1` definition,
# that field still undergoes the parent transformation (defined by `my-schema@1`) where it is constrained
# to `d::Int`.
@test_throws InexactError MyChildRow(Tables.rowmerge(child; d=1.5))

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

# This highlights two important properties regarding `Legolas.Schema` validation:
#
# - First, it's okay that the `e` field isn't present in this `Tables.Schema` because `my-schema` permits `e::Missing`.
# - Second, field ordering is unimportant and is not considered when determining whether a give `Tables.`
@test (Legolas.validate(Tables.Schema((:c, :d, :a, :b), Tuple{Vector,Int,Float64,String}), schema); true)

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
msg = """
      could not extract valid `Legolas.Schema` from provided Arrow table;
      is it missing the expected custom metadata and/or the expected
      \"legolas_schema_qualified\" field?
      """
@test_throws ArgumentError(msg) Legolas.read(Arrow.tobuffer(rows))
invalid_but_has_schema_metadata = Arrow.tobuffer(invalid;
                                                 metadata = ("legolas_schema_qualified" => Legolas.schema_qualified_string(schema),))
@test_throws ArgumentError("field `a` has unexpected type; expected <:Real, found Union{Missing, Float64, String}") Legolas.read(invalid_but_has_schema_metadata)

# A note about one additional benefit of `Legolas.read`/`Legolas.write`: Unlike their Arrow.jl counterparts,
# these functions are relatively agnostic to the types of provided path arguments. Generally, as long as a
# given `path` supports `Base.read(path)::Vector{UInt8}` and `Base.write(path, bytes::Vector{UInt8})` then
# `path` will work as an argument to `Legolas.read`/`Legolas.write`. At some point, we'd like to make similar
# upstream improvements to Arrow.jl to render its API more path-type-agnostic.
=#
