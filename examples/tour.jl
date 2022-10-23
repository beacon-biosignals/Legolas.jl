# This file provides an introductory tour for Legolas.jl. Run lines in the REPL
# to inspect output at each step! Tests are littered throughout to demonstrate
# functionality in a concrete manner, and so that we can ensure examples stay
# current as the package evolves.

using Legolas, Arrow, Tables, Test

using Legolas: @schema, @version, complies_with, find_violation, validate

#####
##### Introduction
#####

# Legolas provides mechanisms for constructing, reading, writing, and validating
# Tables.jl-compliant values against extensible, versioned, user-specified *schemas*.

# We'll dive into the extensibility and versioning aspects of Legolas later. For now,
# let's start the tour by declaring a new Legolas schema via the `@schema` macro.

# Here, we declare a new schema named `example.foo`, specifying that Legolas should
# use the prefix `Foo` whenever it generates `example.foo`-related type definitions:
@schema("example.foo", Foo)

# The above schema declaration provides the necessary scaffolding to start declaring
# new *versions* of the `example.foo` schema. Schema version declarations specify the
# set of required fields that a given table (or row) must contain in order to comply
# with that schema version. Let's use the `@version` macro to declare an initial
# version of the `example.foo` schema with some required fields:
@version "example.foo@1" begin
    a::Real
    b::String
    c
    d::AbstractVector
end

# Behind the scenes, this `@version` declaration automatically generated some type definitions
# and overloaded a bunch of useful Legolas methods with respect to `example.foo@1`. One of the
# types it generated is `FooSchemaV1`, an alias for `Legolas.SchemaVersion`:
@test FooSchemaV1() == Legolas.SchemaVersion("example.foo", 1)

#####
##### `Tables.Schema` Compliance/Validation
#####

# We can use `complies_with`, `validate`, and `find_violation` to check whether a given
# `Tables.Schema` (ref https://tables.juliadata.org/stable/#Tables.Schema) complies with
# `example.foo@1`.

# For example, all of the following `Tables.Schema`s comply with `example.foo@1`:
for s in [Tables.Schema((:a, :b, :c, :d), (Real, String, Any, AbstractVector)), # All required fields must be present...
          Tables.Schema((:a, :b, :c, :d), (Int, String, Float64, Vector)),      # ...and have subtypes that match the schema's declared type constraints.
          Tables.Schema((:b, :a, :d, :c), (String, Int, Vector, Float64)),      # Fields do not have to be in any particular order, as long as they are present.
          Tables.Schema((:a, :b, :d), (Int, String, Vector)),                   # Fields whose declared type constraints are `>:Missing` may be elided entirely.
          Tables.Schema((:a, :x, :b, :y, :d), (Int, Any, String, Any, Vector))] # Non-required fields may also be present.
    # if `complies_with` finds a violation, it returns `false`; returns `true` otherwise
    @test complies_with(s, FooSchemaV1())

    # if `validate` finds a violation, it throws an error indicating the violation;
    # returns `nothing` otherwise
    @test validate(s, FooSchemaV1()) isa Nothing

    # if `find_violation` finds a violation, it returns a tuple indicating the relevant
    # field and its violation; returns `nothing` otherwise
    @test isnothing(find_violation(s, FooSchemaV1()))
end

# ...while the below `Tables.Schema`s do not:

s = Tables.Schema((:a, :c, :d), (Int, Float64, Vector)) # The required non-`>:Missing` field `b::String` is not present.
@test !complies_with(s, FooSchemaV1())
@test_throws ArgumentError validate(s, FooSchemaV1())
@test isequal(find_violation(s, FooSchemaV1()), :b => missing)

s = Tables.Schema((:a, :b, :c, :d), (Int, String, Float64, Any)) # The type of required field `d::AbstractVector` is not `<:AbstractVector`.
@test !complies_with(s, FooSchemaV1())
@test_throws ArgumentError validate(s, FooSchemaV1())
@test isequal(find_violation(s, FooSchemaV1()), :d => Any)

# The expectations that characterize Legolas' particular notion of "schematic compliance" - requiring the
# presence of pre-specified declared fields, assuming non-present fields to be implicitly `missing`, and allowing
# the presence of non-required fields - were chosen such that the question "Does the table `t` comply with the Legolas
# schema version `s`?" is roughly equivalent to "Can a logical view be trivially constructed atop table `t` that contains
# only the required fields declared by `s`?" The ability to cleanly ask this question enables a weak notion of "subtyping"
# (see https://en.wikipedia.org/wiki/Duck_typing, https://en.wikipedia.org/wiki/Liskov_substitution_principle) that is
# core to Legolas' mechanisms for defining, extending, and versioning interfaces to tabular data.

#####
##### Legolas-Generated Record Types
#####
# In addition to `FooSchemaV1`, `example.foo@1`'s `@version` declaration also generated a new type,
# `FooV1 <: Tables.AbstractRow`, whose fields are guaranteed to match all the fields required by
# `example.foo@1`. We refer to such Legolas-generated types as "Legolas record types" (see
# https://en.wikipedia.org/wiki/Record_(computer_science)).

# Legolas record type constructors accept keyword arguments or `Tables.AbstractRow`-compliant values:
fields = (a=1.0, b="hi", c=π, d=[1, 2, 3])
@test NamedTuple(FooV1(; fields...)) == fields
@test NamedTuple(FooV1(fields)) == fields

# This may seem like a fairly trivial constructor in the preceding example, but it has some properties
# that can be quite convenient in practice. Specifically, row values provided to `FooV1` may:
#
# - ...contain the associated schema version's required fields in any order
# - ...elide required fields, in which case the constructor will assume them to be `missing`
# - ...contain any other fields in addition to the required fields; such additional fields are simply ignored
#
# Demonstrating a few of these properties:

# Providing the additional non-required field `x` in the input, which is simply ignored:
fields_with_x = (; fields..., x="x")
@test NamedTuple(FooV1(fields_with_x)) == fields

# Eliding the required field `c`, which is assigned `missing` in the output:
foo = FooV1(; a=1.0, b="hi", d=[1, 2, 3])
@test isequal(NamedTuple(foo), (a=1.0, b="hi", c=missing, d=[1, 2, 3]))

# Providing the non-compliantly-typed field `d::Int`, inducing a `MethodError`:
@test_throws MethodError FooV1(; a=1.0, b="hi", d=2)

# Implicitly providing the non-compliantly-typed field `d::Missing`, inducing a `MethodError`:
@test_throws MethodError FooV1(; a=1.0, b="hi")

#=
#####
##### Custom Field Assignments
#####

# Schema authors may tailor the behavior of `row` for their schema by defining custom field
# assignments in the schema's declaration. The `example.foo@1` schema declaration doesn't feature
# any such assignments, so let's declare a new schema `example.bar@1` that does:
@schema("example.bar@1",
        x::Union{Int8,Missing} = ismissing(x) ? x : Int8(clamp(x, -128, 127)),
        y::String = string(y),
        z::String = ismissing(z) ? string(y, '_', x) : z)
@alias("example.bar", Bar)

# These statements are then inlined into `Bar{1}`'s generated `row` method
# definition, which becomes roughly equivalent to:
#
#   function Legolas.row(::Bar{1}; x=missing, y=missing, __extra__...)
#       x::Union{Int8,Missing} = ismissing(x) ? x : Int8(clamp(x, -128, 127))
#       y::String = string(y, '_', x)
#       return (; x, y, __extra__...)
#   end
#
# ...such that invocations `row(::Bar{1}; ...)` have the following behavior:
@test row(Bar(1); x=200, y=:hi) == (x=127, y="hi", z="hi_127")
@test isequal(row(Bar(1); y=:hi), (x=missing, y="hi", z="hi_missing"))
@test row(Bar(1); x=200, y=:hi, z="bye") == (x=127, y="hi", z="bye")

# Custom field assignments enable schema authors to enforce value-level constraints and to imbue
# `row` with convenient per-field transformations/conversions so that it can accept a wider range
# of applicable inputs for each field. However, schema authors that use custom field assignments
# must always take care to preserve idempotency and avoid side effects / reliance on non-local
# state.
#
# In other words, **the following equivalences should always hold for non-pathological schemas**:
#
#   row(schema, fields) == row(schema, fields)
#   row(schema, row(schema, fields)) == row(schema, fields)
#
# These are two very important expectations that must be met for `row` to behave as intended,
# but they are not enforceable by Legolas itself, since Legolas allows custom field assignments
# to include arbitrary Julia code; thus, schema authors are responsible for not violating these
# expectations.
#
# Let's check that `Bar{1}` meets these expectations:
fields = (x=200, y=:hi)
@test row(Bar(1), fields) == row(Bar(1), fields)
@test row(Bar(1), row(Bar(1), fields)) == row(Bar(1), fields)

# For illustration's sake, here is an example of a pathological schema declaration that violates
# both of these expectations:
const GLOBAL_STATE = Ref(0)

@schema("example.bad@1",
        x::Int = x + 1,
        y = (GLOBAL_STATE[] += y; GLOBAL_STATE[]))
@alias("example.bad", Bad)

# Demonstration of non-idempotency, both in `x` and `y` fields:
@test row(Bad(1), row(Bad(1), (x=1, y=1))) != row(Bad(1), (x=1, y=1))

# Demonstration of side effects / reliance on non-local state in `y` field:
@test row(Bad(1), (x=1, y=1)) != row(Bad(1), (x=1, y=1))

#####
##### Extending Existing Schemas
#####

# Schemas declared via `@schema` can inherit the required fields specified by previous schema declarations.
# Here, we define the schema `example.baz@1` which "extends" the schema `example.bar@1`:
@schema("example.baz@1 > example.bar@1",
        x::Int8,
        z::String,
        k::Int64 = ismissing(k) ? length(z) : k)
@alias("example.baz", Baz)

# Notice how the child schema's declaration may reference the parent's schema fields (but need not reference
# every single one), may tighten the constraints of the parent schema's required fields, and may introduce
# new required fields atop the parent schema's required fields.

# For a given Legolas schema extension to be valid, all possible row types that comply with the child schema
# must comply with the parent schema, but the reverse need not be true. We can check schemas' required fields
# and their type constraints via `Legolas.schema_fields`. Based on these outputs, it is a worthwhile exercise
# to confirm for yourself that `Baz{1}` is a valid extension of `Bar{1}` under the aforementioned rule:
@test Legolas.schema_fields(Bar(1)) == (x=Union{Missing,Int8}, y=String, z=String)
@test Legolas.schema_fields(Baz(1)) == (x=Int8, y=String, z=String, k=Int64)

# As a counterexample, the following is invalid, because the declaration of `x::Any` would allow for `x`
# values that are disallowed by the parent schema `example.bar@1`:
@test_throws Legolas.SchemaDeclarationError @schema("example.broken@1 > example.bar@1", x::Any)

# When `row` is evaluated against an extension schema, it will apply the parent schema's field
# assignments before applying the child schema's field assignments. Notice how `row` applies the
# constraints/transformations of the parent and child schemas in the below examples:
@test row(Baz(1); x=200, y=:hi) == (x=127, z="hi_127", k=6, y="hi")
@test_throws MethodError row(Baz(1); y=:hi) # `Baz{1}` does not allow `x::Missing`

# `Baz{1}`'s generated `row` method definition is thus roughly equivalent to:
#
#   function Legolas.row(::Baz{1}; fields...)
#       fields = row(Bar(1), fields)
#       return Legolas._row(Baz{1}; fields...)
#   end
#
#   function Legolas._row(::Baz{1}; x=missing, z=missing, k=missing, __extra__...)
#       x::Int8 = x
#       z::String = z
#       k::Int = length(z)
#       return (; x, z, k, __extra__...)
#   end

# One last note on syntax: You might ask "Why use `>` as the inheritance operator instead of `<:`?" There are two reasons.
# The primary reason is purely historical: earlier versions of Legolas did not as rigorously demand/enforce subtyping
# relationships between parent and child schemas' required fields, and so the `<:` operator was considered to be a bit
# too misleading. A secondary reason in favor of `>` was that it implied the actual order of application of field
# constraints/transformations (i.e. the parent's are applied before the child's).

#####
##### Schema Versioning
#####

# Throughout this tour, every schema declaration/instance has included a mysterious integer that we've referred to
# nonchalantly as that schema's "version". We're not going to dive into this aspect of Legolas here in the tour, but
# please refer to this section of Legolas' documentation for more details:
# https://beacon-biosignals.github.io/Legolas.jl/stable/schema-concepts/#Schema-Versioning:-You-Break-It,-You-Bump-It-1

#####
##### Validating/Writing/Reading Arrow Tables with Legolas.jl
#####

# Legolas provides special methods for reading/writing/validating Arrow tables that utilize `Legolas.Schema`s. To
# start exploring these methods, we'll first construct a dummy table using the previously defined `Baz{1}` schema:
baz = row(Baz(1); x=200, y=:hi)
invalid = [baz,
           Tables.rowmerge(baz; k="violates `k::Int64`"),
           Tables.rowmerge(baz; z="some_other_value")]

# Oops, this table doesn't actually comply with `Baz{1}`! We can find the violation via the same
# `find_violation` that we utilized earlier in the tour. Note our use of `Legolas.guess_schema`
# here, which is similar to `Tables.schema` but does a little bit of extra work in order to try
# to figure out the input's `Table.Schema` even if it's not evident from the table's type.
@test find_violation(Legolas.guess_schema(invalid), Baz(1)) == (:k => Any)

# Let's fix the violation:
table = [invalid[1], Tables.rowmerge(baz; k=123), invalid[3]]

# Much better:
@test complies_with(Legolas.guess_schema(table), Baz(1))

# Legolas provides dedicated read/write functions that can perform similar schema validation while
# (de)serializing tables to/from Arrow.

# `Legolas.write(io_or_path, table, schema; kwargs...)` wraps `Arrow.write(io_or_path, table; kwargs...)`
# and performs two additional operations atop the usual operations performed by that function:
#
# - By default, the provided Tables.jl-compliant `table` is validated against `schema` via `Legolas.validate` before
#   it is actually written out. Note that this can be disabled by passing `validate=false` to `Legolas.write`.
#
# - `Legolas.write` ensures that the written-out Arrow table's metadata contains a `"legolas_schema_qualified"`
#   key whose value is `Legolas.schema_identifier(schema)`. This field enables consumers of the table to
#   perform automated (or manual) schema discovery/evolution/validation.
table_isequal(a, b) = isequal(Legolas.materialize(a), Legolas.materialize(b))

io = IOBuffer()
Legolas.write(io, table, Baz(1))
t = Arrow.Table(seekstart(io))
@test Arrow.getmetadata(t) == Dict("legolas_schema_qualified" => "example.baz@1>example.bar@1")
@test table_isequal(t, Arrow.Table(Arrow.tobuffer(table)))
@test table_isequal(t, Arrow.Table(Legolas.tobuffer(table, Baz(1)))) # `Legolas.tobuffer` is analogous to `Arrow.tobuffer`

# Similarly, Legolas provides `Legolas.read(path_or_io)`, which wraps `Arrow.Table(path_or_io)`
# validates the deserialized `Arrow.Table` before returning it:
@test table_isequal(Legolas.read(Legolas.tobuffer(table, Baz(1))), t)
msg = """
      could not extract valid `Legolas.Schema` from the `Arrow.Table` read
      via `Legolas.read`; is it missing the expected custom metadata and/or
      the expected \"legolas_schema_qualified\" field?
      """
@test_throws ArgumentError(msg) Legolas.read(Arrow.tobuffer(table))
invalid_but_has_schema_metadata = Arrow.tobuffer(invalid; metadata=("legolas_schema_qualified" => Legolas.schema_identifier(Baz(1)),))
@test_throws ArgumentError("field `k` has unexpected type; expected <:Int64, found Union{Missing, Int64, String}") Legolas.read(invalid_but_has_schema_metadata)

# A note about one additional benefit of `Legolas.read`/`Legolas.write`: Unlike their Arrow.jl counterparts,
# these functions are relatively agnostic to the types of provided path arguments. Generally, as long as a
# given `path` supports `Base.read(path)::Vector{UInt8}` and `Base.write(path, bytes::Vector{UInt8})` then
# `path` will work as an argument to `Legolas.read`/`Legolas.write`. At some point, we'd like to make similar
# upstream improvements to Arrow.jl to render its API more path-type-agnostic.
=#
