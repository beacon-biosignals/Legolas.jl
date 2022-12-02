# This file provides an introductory tour for Legolas.jl. Run lines in the REPL
# to inspect output at each step! Tests are littered throughout to demonstrate
# functionality in a concrete manner, and so that we can ensure examples stay
# current as the package evolves.

using Legolas, Arrow, Tables, Test, UUIDs

using Legolas: @schema, @version, complies_with, find_violation, validate

#####
##### Introduction
#####

# Legolas provides mechanisms for constructing, reading, writing, and validating
# Tables.jl-compliant values against extensible, versioned, user-specified *schemas*.

# We'll dive into the extensibility and versioning aspects of Legolas later. For now,
# let's start the tour by declaring a new Legolas schema via the `@schema` macro.

# Here, we declare a new schema named `example.foo`, specifying that Legolas should
# use the prefix `Foo` for all `example.foo`-related type definitions:
@schema "example.foo" Foo

# The above schema declaration provides the necessary scaffolding to start declaring
# new *versions* of the `example.foo` schema. Schema version declarations specify the
# set of required fields that a given table (or row) must contain in order to comply
# with that schema version. Let's use the `@version` macro to declare an initial
# version of the `example.foo` schema with some required fields:
@version FooV1 begin
    a::Real
    b::String
    c
    d::AbstractVector
end

# In the above declaration, the symbol `FooV1` can be broken into the prefix `Foo` (as
# specified in `example.foo`'s `@schema` declaration) and `1`, the integer that identifies
# this particular version of the `example.foo` schema. The `@version` macro requires this
# symbol to always follow this format (`$(prefix)V$(integer)`), because it generates two
# special types that match it. For example, our `@version` declaration above generated:
#
# - `FooV1`: A special subtype of `Tables.AbstractRow` whose fields match the corresponding
#   schema version's declared required fields.
# - `FooV1SchemaVersion`: An alias for `Legolas.SchemaVersion` that matches the corresponding
#   schema version.

# Let's first examine `FooV1SchemaVersion`:
@test Legolas.SchemaVersion("example.foo", 1) == FooV1SchemaVersion()
@test Legolas.SchemaVersion("example.foo", 1) isa FooV1SchemaVersion
@test "example.foo@1" == Legolas.identifier(FooV1SchemaVersion())

# As you can see, Legolas' Julia-agnostic identifier for this schema version is `example.foo@1`.
# To avoid confusion throughout this tour, we'll use this Julia-agnostic identifier to refer to
# individual schema versions in the abstract sense, while we'll use the relevant `SchemaVersion`
# aliases to specifically refer to the types that represent schema versions in Julia.

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
    @test complies_with(s, FooV1SchemaVersion())

    # if `validate` finds a violation, it throws an error indicating the violation;
    # returns `nothing` otherwise
    @test validate(s, FooV1SchemaVersion()) isa Nothing

    # if `find_violation` finds a violation, it returns a tuple indicating the relevant
    # field and its violation; returns `nothing` otherwise
    @test isnothing(find_violation(s, FooV1SchemaVersion()))
end

# ...while the below `Tables.Schema`s do not:

s = Tables.Schema((:a, :c, :d), (Int, Float64, Vector)) # The required non-`>:Missing` field `b::String` is not present.
@test !complies_with(s, FooV1SchemaVersion())
@test_throws ArgumentError validate(s, FooV1SchemaVersion())
@test isequal(find_violation(s, FooV1SchemaVersion()), :b => missing)

s = Tables.Schema((:a, :b, :c, :d), (Int, String, Float64, Any)) # The type of required field `d::AbstractVector` is not `<:AbstractVector`.
@test !complies_with(s, FooV1SchemaVersion())
@test_throws ArgumentError validate(s, FooV1SchemaVersion())
@test isequal(find_violation(s, FooV1SchemaVersion()), :d => Any)

# The expectations that characterize Legolas' particular notion of "schematic compliance" - requiring the
# presence of pre-specified declared fields, assuming non-present fields to be implicitly `missing`, and allowing
# the presence of non-required fields - were chosen such that the question "Does the table `t` comply with the Legolas
# schema version `s`?" is roughly equivalent to "Can a logical view be trivially constructed atop table `t` that contains
# only the required fields declared by `s`?". The ability to cleanly ask this question enables a weak notion of "subtyping"
# (see https://en.wikipedia.org/wiki/Duck_typing, https://en.wikipedia.org/wiki/Liskov_substitution_principle) that is
# core to Legolas' mechanisms for defining, extending, and versioning interfaces to tabular data.

#####
##### Legolas-Generated Record Types
#####

# As mentioned in this tour's introduction, `FooV1` is a subtype of `Tables.AbstractRow` whose fields are guaranteed to
# match all the fields required by `example.foo@1`. We refer to such Legolas-generated types as "record types" (see
# https://en.wikipedia.org/wiki/Record_(computer_science)). These record types are direct subtypes of
# `Legolas.AbstractRecord`, which is, itself, a subtype of `Tables.AbstractRow`:
@test FooV1 <: Legolas.AbstractRecord <: Tables.AbstractRow

# Record type constructors accept keyword arguments or `Tables.AbstractRow`-compliant values:
fields = (a=1.0, b="hi", c=π, d=[1, 2, 3])
@test NamedTuple(FooV1(; fields...)) == fields
@test NamedTuple(FooV1(fields)) == fields

# This may seem like a fairly trivial constructor in the preceding example, but it has some properties
# that can be quite convenient in practice. Specifically, row values provided to `FooV1` may:
#
# - ...contain the associated schema version's required fields in any order
# - ...elide required fields, in which case the constructor will assume them to be `missing`
# - ...contain any other fields in addition to the required fields; such additional fields are simply dropped
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

#####
##### Custom Field Assignments
#####

# Schema authors may tailor the behavior of `row` for their schema by defining custom field
# assignments in the schema's declaration. The `example.foo@1` declaration doesn't feature
# any such assignments, so let's declare a new schema version `example.bar@1` that does:
@schema "example.bar" Bar

@version BarV1 begin
    x::Union{Int8,Missing} = ismissing(x) ? x : Int8(clamp(x, -128, 127))
    y::String = string(y)
    z::String = ismissing(z) ? string(y, '_', x) : z
end

# These assignment statements are inlined into `BarV1`'s inner constructor
# definition, such that it is roughly equivalent to:
#
#   function BarV1(; x=missing, y=missing)
#       x::Union{Int8,Missing} = ismissing(x) ? x : Int8(clamp(x, -128, 127))
#       y::String = string(y, '_', x)
#       return new(x, y)
#   end
#
# ...so that invocations `BarV1(; ...)` have the following behavior:
@test NamedTuple(BarV1(; x=200, y=:hi)) == (x=127, y="hi", z="hi_127")
@test isequal(NamedTuple(BarV1(; y=:hi)), (x=missing, y="hi", z="hi_missing"))
@test NamedTuple(BarV1(; x=200, y=:hi, z="bye")) == (x=127, y="hi", z="bye")

# Custom field assignments enable schema authors to enforce value-level constraints and to imbue
# record type constructors with convenient per-field transformations/conversions so that they can
# accept a wider range of applicable inputs for each field. However, schema authors that use custom
# field assignments must always take care to preserve idempotency and avoid side effects / reliance
# on non-local state.
#
# In other words, given a record type `R` generated from a non-pathological `@version` declaration,
# we'd expect the following equivalences to hold:
#
#   R(fields) == R(fields)
#   R(R(fields)) == R(fields)
#
# These are two very important expectations that must be met for record types to behave as intended,
# but they are not enforceable by Legolas itself, since Legolas allows custom field assignments
# to include arbitrary Julia code; thus, schema authors are responsible for not violating these
# expectations.
#
# Let's check that `BarV1` meets these expectations:
fields = (x=200, y=:hi)
@test BarV1(fields) == BarV1(fields)
@test BarV1(BarV1(fields)) == BarV1(fields)

# For illustration's sake, here is an example of a pathological `@version` declaration that violates
# both of these expectations:
const GLOBAL_STATE = Ref(0)

@schema "example.bad" Bad

@version BadV1 begin
    x::Int = x + 1
    y = (GLOBAL_STATE[] += y; GLOBAL_STATE[])
end

fields = (x=1, y=1)

# Demonstration of non-idempotency, both in `x` and `y` fields:
@test BadV1(BadV1(fields)) != BadV1(fields)

# Demonstration of side effects / reliance on non-local state in `y` field:
@test BadV1(fields) != BadV1(fields)

#####
##### Extending Existing Schema Versions
#####

# New schema versions can inherit other schema version's required fields. Here, we declare `example.baz@1`
# as an "extension" of `example.bar@1`:
@schema "example.baz" Baz

@version BazV1 > BarV1 begin
    x::Int8
    z::String
    k::Int64 = ismissing(k) ? length(z) : k
end

# Notice how the child's `@version` declaration may reference the parent's required fields (but need not reference
# every single one), may tighten the constraints of the parent's required fields, and may introduce new required
# fields atop the parent's required fields.

# For a given Legolas schema version extension to be valid, all `Tables.Schema`s that comply with the child
# must comply with the parent, but the reverse need not be true. We can check a schema version's required fields
# and their type constraints via `Legolas.required_fields`. Based on these outputs, it is a worthwhile exercise
# to confirm for yourself that `BazV1SchemaVersion` is a valid extension of `BarV1SchemaVersion` under the aforementioned rule:
@test Legolas.required_fields(BarV1SchemaVersion()) == (x=Union{Missing,Int8}, y=String, z=String)
@test Legolas.required_fields(BazV1SchemaVersion()) == (x=Int8, y=String, z=String, k=Int64)

# As a counterexample, the following is invalid, because the declaration of `x::Any` would allow for `x`
# values that are disallowed by the parent schema version `example.bar@1`:
@schema "example.broken" Broken
@test_throws Legolas.SchemaVersionDeclarationError @version BrokenV1 > BarV1 begin x::Any end

# Record type constructors generated for extension schema versions will apply the parent's field
# assignments before applying the child's field assignments. Notice how `BazV1` applies the
# constraints/transformations of both `example.baz@1` and `example.bar@1`:
@test NamedTuple(BazV1(; x=200, y=:hi)) == (x=127, y="hi", z="hi_127", k=6)
@test_throws MethodError BazV1(; y=:hi) # `example.baz@1` does not allow `x::Missing`

# `BazV1`'s inner constructor definition is roughly equivalent to:
#
#   function BazV1(; x=missing, y=missing, z=missing, k=missing)
#       __p__ = BarV1(; x, y, z)
#       x, y, z = __p__.x, __p__.y, __p__.z
#       x::Int8 = x
#       z::String = z
#       k::Int = length(z)
#       return new(x, y, z, k)
#   end

# One last note on syntax: You might ask "Why use the greater-than symbol as the inheritance operator instead of `<:`?"
# There are a few reasons. The primary reason is purely historical: earlier versions of Legolas did not as rigorously
# demand/enforce subtyping relationships between parent and child schemas' required fields, and so the `<:` operator
# was considered to be a bit too misleading. A secondary reason in favor of `>` was that it implied the actual order
# of application of constraints (i.e. the parent's are applied before the child's). Lastly, `>` aligns well with the
# property that child schema versions have a greater number of constraints than their parents.

#####
##### Schema Versioning
#####

# Throughout this tour, all `@version` declarations have used the version number `1`. As you might guess, you can
# declare more than a single version of any given schema. Here's an example using the `example.foo` schema we defined
# earlier:

@version FooV2 begin
    a::Float64
    b::String
    c::Int
    d::Vector
end

@test FooV2SchemaVersion() == Legolas.SchemaVersion("example.foo", 2)

fields = (a=1.0, b="b", c=3, d=[1,2,3])
@test NamedTuple(FooV2(fields)) == fields

# A schema author generally needs to declare a new schema version whenever they introduce that changes are
# considered "breaking" in a very particular Legolas-defined manner. We're not going to dive into this aspect
# of Legolas here in the tour, but please refer to this section of Legolas' documentation for more details:
# https://beacon-biosignals.github.io/Legolas.jl/stable/schema-concepts/#Schema-Versioning:-You-Break-It,-You-Bump-It-1

#####
##### Parameterizing Required Field Types
#####

# Sometimes, it's useful to surface a required field's type as a type parameter of the generated record type. To
# achieve this, the `@version` macro supports use of the `<:` operator to mark fields whose types should be exposed
# as parameters. For example:

@schema "example.param" Param

@version ParamV1 begin
    a::Int
    b::(<:Real)
    c
    d::(<:Union{Real,Missing})
end

@test typeof(ParamV1(a=1, b=2.0, c="3")) === ParamV1{Float64,Missing}
@test typeof(ParamV1(a=1, b=2.0, c="3", d=4)) === ParamV1{Float64,Int}
@test typeof(ParamV1{Int,Missing}(a=1, b=2.0, c="3")) === ParamV1{Int,Missing}
@test typeof(ParamV1{Int,Float32}(a=1, b=2.0, c="3", d=1)) === ParamV1{Int,Float32}

# Note that extension schema versions do not implicitly "inherit" their parent's type parameters; if you'd
# like to parameterize the type of a parent's required field in the child schema version, you should explicitly
# include the field in the child's required field list:

@schema "example.child-param" ChildParam

@version ChildParamV1 > ParamV1 begin
    c::(<:Union{Real,String})
    d::(<:Union{Real,Missing})
    e
end

@test typeof(ChildParamV1(a=1, b=2.0, c="3", e=5)) === ChildParamV1{String,Missing}
@test typeof(ChildParamV1(a=1, b=2.0, c=3, d=4, e=5)) === ChildParamV1{Int,Int}
@test typeof(ChildParamV1{Int,Missing}(a=1, b=2.0, c=3.0, e=5)) === ChildParamV1{Int,Missing}
@test typeof(ChildParamV1{String,Float32}(a=1, b=2.0, c="3", d=1, e=5)) === ChildParamV1{String,Float32}

#####
##### Validating/Writing/Reading Arrow Tables with Legolas.jl
#####

# Legolas provides special methods for reading/writing/validating Arrow tables with `Legolas.SchemaVersion`s.
# To start exploring these methods, we'll first construct a dummy table using the previously defined `BazV1`:
table = [BazV1(; x=23, y=:beep),
         BazV1(; x=200, y=:boop, k=4),
         BazV1(; x=23, y=:buzz, z="some_other_value")]

table_isequal(a, b) = isequal(Legolas.materialize(a), Legolas.materialize(b))

# `Legolas.write(dest, table, sv::Legolas.SchemaVersion; kwargs...)` wraps `Arrow.write(dest, table; kwargs...)`
# and performs two additional operations atop the usual operations performed by that function:
#
# - By default, the provided Tables.jl-compliant `table` is validated against `sv` via `Legolas.validate` before
#   it is actually written out. Note that this can be disabled by passing `validate=false` to `Legolas.write`.
#
# - `Legolas.write` ensures that the written-out Arrow table's metadata contains a `"legolas_schema_qualified"`
#   key whose value is `Legolas.schema_identifier(schema)`. This field enables consumers of the table to
#   perform automated (or manual) schema discovery/evolution/validation.
io = IOBuffer()
Legolas.write(io, table, BazV1SchemaVersion())
t = Arrow.Table(seekstart(io))
@test Arrow.getmetadata(t) == Dict("legolas_schema_qualified" => "example.baz@1>example.bar@1")
@test table_isequal(t, Arrow.Table(Arrow.tobuffer(table)))
@test table_isequal(t, Arrow.Table(Legolas.tobuffer(table, BazV1SchemaVersion()))) # `Legolas.tobuffer` is analogous to `Arrow.tobuffer`

# Similarly, Legolas provides `Legolas.read(src)`, which wraps `Arrow.Table(src)`, but
# validates the deserialized `Arrow.Table` against its declared schema version before
# returning it:
@test table_isequal(Legolas.read(Legolas.tobuffer(table, BazV1SchemaVersion())), t)
msg = """
      could not extract valid `Legolas.SchemaVersion` from the `Arrow.Table` read
      via `Legolas.read`; is it missing the expected custom metadata and/or the
      expected \"legolas_schema_qualified\" field?
      """
@test_throws ArgumentError(msg) Legolas.read(Arrow.tobuffer(table))
invalid = [Tables.rowmerge(row; k=string(row.k)) for row in table]
invalid_but_has_metadata = Arrow.tobuffer(invalid; metadata=("legolas_schema_qualified" => Legolas.identifier(BazV1SchemaVersion()),))
@test_throws ArgumentError("field `k` has unexpected type; expected <:Int64, found String") Legolas.read(invalid_but_has_metadata)

# A note about one additional benefit of `Legolas.read`/`Legolas.write`: Unlike their Arrow.jl counterparts,
# these functions are relatively agnostic to the types of provided path arguments. Generally, as long as a
# given `path` supports `Base.read(path)::Vector{UInt8}` and `Base.write(path, bytes::Vector{UInt8})` then
# `path` will work as an argument to `Legolas.read`/`Legolas.write`. At some point, we'd like to make similar
# upstream improvements to Arrow.jl to render its API more path-type-agnostic.

#####
##### Schema Version Portability (`Legolas.accepted_field_type`)
#####

# Consider the following schema version:

@schema "example.portable" Portable

@version PortableV1 begin
    id::UUID = UUID(id)
end

# Here, `PortableV1` will convert inputs into `UUID`s as part of construction. This behavior may be desirable in many cases,
# but this definition actually has interesting implications for this schema's notion of compliance. In particular, this schema
# version carries an implicit requirement that schema-compliant Arrow data must be Julia-produced; Arrow itself doesn't define
# a native UUID type, so other languages may very well (de)serialize UUIDs as 128-bit binary values in a manner that Arrow.jl
# might not recognize as Julia's UUID type.

# Thus, while this schema version implies that the only compliant `Tables.Schema` is `Tables.Schema((:id,), (UUID,))`,
# it is actually desirable to also consider `Tables.Schema((:id,), (UInt128,))` to be compliant in order to support
# non-Julia-produced data. It'd be annoying, though, to need to alter our `PortableV1` constructor just to suit this purpose,
# since its UUID conversion behavior (and the corresponding type constraint) may be useful for validated construction.

# Luckily, it turns out that Legolas is actually smart enough to natively support this by default:
@test complies_with(Tables.Schema((:id,), (UUID,)), PortableV1SchemaVersion())
@test complies_with(Tables.Schema((:id,), (UInt128,)), PortableV1SchemaVersion())

# How is this possible? Well, when Legolas checks whether a given field `f::T` matches a required field `f::F`, it doesn't
# directly check that `T <: F`; instead, it checks that `T <: Legolas.accepted_field_type(sv, F)` where `sv` is the relevant
# `SchemaVersion`. The fallback definition of `Legolas.accepted_field_type(::SchemaVersion, F::Type)` is simply `F`, but there
# are a few other default overloads to support common Base types that can cause portability issues:
#
#    accepted_field_type(::SchemaVersion, ::Type{UUID}) = Union{UUID,UInt128}
#    accepted_field_type(::SchemaVersion, ::Type{Symbol}) = Union{Symbol,String}
#
# Schema version authors should feel free to override these `Legolas.accepted_field_type` definitions (and/or add new definitions)
# for their own `SchemaVersion` types.
