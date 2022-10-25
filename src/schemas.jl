#####
##### schema name/identifier parsing/validation
#####

const ALLOWED_SCHEMA_NAME_CHARACTERS = Char['-', '.', 'a':'z'..., '0':'9'...]

"""
    Legolas.is_valid_schema_name(x::AbstractString)

Return `true` if `x` is a valid schema name, return `false` otherwise.

Valid schema names are lowercase, alphanumeric, and may contain hyphens or periods.
"""
is_valid_schema_name(x::AbstractString) = all(i -> i in ALLOWED_SCHEMA_NAME_CHARACTERS, x)

#####
##### `SchemaVersion`
#####

"""
    Legolas.SchemaVersion{name,version}

A type representing a particular version of Legolas schema. The relevant `name` (a `Symbol`)
and `version` (an `Integer`) are surfaced as type parameters, allowing them to be utilized for
dispatch.

For more details and examples, please see `Legolas.jl/examples/tour.jl` and the
"Schema-Related Concepts/Conventions" section of the Legolas.jl documentation.

The constructor `SchemaVersion{name,version}()` will throw an `ArgumentError` if `version` is
not non-negative.

See also: [`Legolas.@schema`](@ref)
"""
struct SchemaVersion{n,v}
    function SchemaVersion{n,v}() where {n,v}
        v isa Integer && v >= 0 || throw(ArgumentError("`version` in `SchemaVersion{_,version}` must be a non-negative integer, received: $v"))
        return new{n,v}()
    end
end

"""
    Legolas.SchemaVersion(name::AbstractString, version::Integer)

Return `Legolas.SchemaVersion{Symbol(name),version}()`.

Throws an `ArgumentError` if `name` is not a valid schema name.

Prefer using this constructor over `Legolas.SchemaVersion{Symbol(name),version}()` directly.
"""
function SchemaVersion(n::AbstractString, v::Integer)
    is_valid_schema_name(n) || throw(ArgumentError("argument is not a valid `Legolas.SchemaVersion` name: \"$n\""))
    return SchemaVersion{Symbol(n),v}()
end

SchemaVersion(sv::SchemaVersion) = sv

#####
##### `parse_identifier`
#####

"""
    Legolas.parse_identifier(id::AbstractString)

Given a valid schema version identifier `id` of the form:

    \$(names[1])@\$(versions[1]) > \$(names[2])@\$(versions[2]) > ... > \$(names[n])@\$(versions[n])

return an `n` element `Vector{SchemaVersion}` whose `i`th element is `SchemaVersion(names[i], versions[i])`.

Throws an `ArgumentError` if the provided string is not a valid schema version identifier.

For details regarding valid schema version identifiers and their structure, see the
"Schema-Related Concepts/Conventions" section of the Legolas.jl documentation.
"""
function parse_identifier(id::AbstractString)
    name_and_version_per_schema = [split(strip(x), '@') for x in split(id, '>')]
    results = SchemaVersion[]
    invalid = isempty(name_and_version_per_schema)
    if !invalid
        for nv in name_and_version_per_schema
            if length(nv) != 2
                invalid = true
                break
            end
            n, v = nv
            v = tryparse(Int, v)
            v isa Int && push!(results, SchemaVersion(n, v))
        end
    end
    (invalid || isempty(results)) && throw(ArgumentError("failed to parse seemingly invalid/malformed schema version identifier string: \"$id\""))
    return results
end

#####
##### `UnknownSchemaError`
#####

struct UnknownSchemaError <: Exception
    name::String
    version::Union{Int,Nothing}
end

UnknownSchemaError(sv::SchemaVersion) = UnknownSchemaError(name(sv), version(sv))

function Base.showerror(io::IO, e::UnknownSchemaError)
    print(io, "UnknownSchemaError: encountered unknown Legolas schema with name=\"$(e.name)\"")
    isnothing(e.version) || print(io, ", version=$(e.version)")
    print(io, """

              This generally indicates that this schema has not been declared (i.e.
              the corresponding `@schema` and/or `@version` statements have not been
              executed) in the current Julia session.

              In practice, this can arise if you try to read a Legolas table with a
              prescribed schema, but haven't actually loaded the schema definition
              (or commonly, haven't loaded the dependency that contains the schema
              definition - check the versions of loaded packages/modules to confirm
              your environment is as expected).

              Note that if you're in this particular situation, you can still load the raw
              table as-is without Legolas (e.g. via `Arrow.Table(path_to_table)`).
              """)
    return nothing
end

#####
##### `SchemaVersion` accessors
#####

"""
    Legolas.name(::Legolas.SchemaVersion{n})

Return `n`.
"""
@inline name(::SchemaVersion{n}) where {n} = n

"""
    Legolas.version(::Legolas.SchemaVersion{n,v})

Return `v`.
"""
@inline version(::SchemaVersion{n,v}) where {n,v} = v

"""
    Legolas.parent(sv::Legolas.SchemaVersion)

Return the `Legolas.SchemaVersion` instance that corresponds to `sv`'s declared parent.
"""
@inline parent(::SchemaVersion) = nothing

"""
    Legolas.declared(sv::Legolas.SchemaVersion{name,version})

Return `true` if the schema version `name@version` has been declared via `@version` in the current Julia
session; return `false` otherwise.
"""
@inline declared(::SchemaVersion) = false

"""
    Legolas.identifier(::Legolas.SchemaVersion)

Return this `Legolas.SchemaVersion`'s fully qualified schema version identifier. This string is serialized
as the `\"$LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY\"` field value in table metadata for table
written via [`Legolas.write`](@ref).
"""
identifier(sv::SchemaVersion) = throw(UnknownSchemaError(sv))

"""
    Legolas.required_fields(sv::Legolas.SchemaVersion)

Return a `NamedTuple{...,Tuple{Vararg{DataType}}` whose fields take the form:

    <name of field required by `sv`> = <field's type>

If `sv` has a parent, the returned fields will include `required_fields(parent(sv))`.
"""
required_fields(sv::SchemaVersion) = throw(UnknownSchemaError(sv))

"""
    Legolas.declaration(sv::Legolas.SchemaVersion)

Return a `Pair{String,Vector{NamedTuple}}` of the form

    schema_identifier_used_for_declaration::String => required_field_infos::Vector{Legolas.RequiredFieldInfo}

where `RequiredFieldInfo` has the fields:

- `name::Symbol`: the required field's name
- `type::Union{Symbol,Expr}`: the required field's declared type constraint
- `parameterize::Bool`: whether or not the required field is exposed as a parameter
- `statement::Expr`: the required field's full assignment statement (as processed by `@version`, not necessarily as written)

Note that `declaration` is primarily intended to be used for interactive discovery purposes, and
does not include the contents of `declaration(parent(sv))`.
"""
declaration(sv::SchemaVersion) = throw(UnknownSchemaError(sv))

#####
##### `SchemaVersion` Arrow (de)serialization
#####

const LEGOLAS_SCHEMA_VERSION_ARROW_NAME = Symbol("JuliaLang.Legolas.SchemaVersion")
Arrow.ArrowTypes.arrowname(::Type{<:SchemaVersion}) = LEGOLAS_SCHEMA_VERSION_ARROW_NAME
Arrow.ArrowTypes.ArrowType(::Type{<:SchemaVersion}) = String
Arrow.ArrowTypes.toarrow(sv::SchemaVersion) = identifier(sv)
Arrow.ArrowTypes.JuliaType(::Val{LEGOLAS_SCHEMA_VERSION_ARROW_NAME}, ::Any) = SchemaVersion
Arrow.ArrowTypes.fromarrow(::Type{<:SchemaVersion}, id) = first(parse_identifier(id))

#####
##### `Tables.Schema` validation
#####

@inline accepted_field_type(::SchemaVersion, T) = T
accepted_field_type(::SchemaVersion, ::Type{UUID}) = Union{UUID,UInt128}

"""
    Legolas.find_violation(ts::Tables.Schema, sv::Legolas.SchemaVersion)

For required field `f::F` of `sv`:

- Define `A = Legolas.accepted_field_type(F)`
- If `f::T` is present in `ts`, ensure that `T <: A` or else immediately return `f::Symbol => T::DataType`.
- If `f` isn't present in `ts`, ensure that `Missing <: A` or else immediately return `f::Symbol => missing::Missing`.

Otherwise, return `nothing`.

See also: [`Legolas.validate`](@ref), [`Legolas.complies_with`](@ref)
"""
find_violation(::Tables.Schema, sv::SchemaVersion) = throw(UnknownSchemaError(sv))

function _find_violation end

"""
    Legolas.validate(ts::Tables.Schema, sv::Legolas.SchemaVersion)

Throws a descriptive `ArgumentError` if `!isnothing(find_violation(ts, sv))`,
otherwise return `nothing`.

See also: [`Legolas.find_violation`](@ref), [`Legolas.complies_with`](@ref)
"""
function validate(ts::Tables.Schema, sv::SchemaVersion)
    result = find_violation(ts, sv)
    isnothing(result) && return nothing
    field, violation = result
    ismissing(violation) && throw(ArgumentError("could not find expected field `$field` in $ts"))
    expected = getfield(required_fields(sv), field)
    throw(ArgumentError("field `$field` has unexpected type; expected <:$expected, found $violation"))
end

"""
    Legolas.complies_with(ts::Tables.Schema, sv::Legolas.SchemaVersion)

Return `isnothing(find_violation(ts, sv))`.

See also: [`Legolas.find_violation`](@ref), [`Legolas.validate`](@ref)
"""
complies_with(ts::Tables.Schema, sv::SchemaVersion) = isnothing(find_violation(ts, sv))

#####
##### `AbstractRecord`
#####

abstract type AbstractRecord <: Tables.AbstractRow end

@inline Tables.getcolumn(r::AbstractRecord, i::Int) = getfield(r, i)
@inline Tables.getcolumn(r::AbstractRecord, nm::Symbol) = getfield(r, nm)
@inline Tables.columnnames(r::AbstractRecord) = fieldnames(typeof(r))
@inline Tables.schema(::AbstractVector{R}) where {R<:AbstractRecord} = Tables.Schema(fieldnames(R), fieldtypes(R))

#####
##### `@schema`
#####

schema_type_prefix(::Val) = nothing

"""
    @schema "name" Prefix

Declare a Legolas schema with the given `name`. Types generated by subsequent
[`@version`](@ref) declarations for this schema will be prefixed with `Prefix`.

For more details and examples, please see `Legolas.jl/examples/tour.jl`.
"""
macro schema(schema_name, schema_prefix)
    schema_name isa String || return :(throw(ArgumentError("`name` provided to `@schema` must be a string literal")))
    occursin('@', schema_name) && return :(throw(ArgumentError("`name` provided to `@schema` should not include an `@` version clause")))
    is_valid_schema_name(schema_name) || return :(throw(ArgumentError("`name` provided to `@schema` is not a valid `Legolas.SchemaVersion` name: \"" * $schema_name * "\"")))
    schema_prefix isa Symbol || return :(throw(ArgumentError(string("`Prefix` provided to `@schema` is not a valid type name: ", $(Base.Meta.quot(schema_prefix))))))
    schema_symbol = Base.Meta.quot(Symbol(schema_name))
    return quote
        Legolas.schema_type_prefix(::Val{$(schema_symbol)}) = $(Base.Meta.quot(schema_prefix))
    end
end

#####
##### `@version`
#####

struct SchemaVersionDeclarationError <: Exception
    message::String
end

function Base.showerror(io::IO, e::SchemaVersionDeclarationError)
    print(io, """
              SchemaVersionDeclarationError: $(e.message)

              Note that valid `@version` declarations meet these expecations:

              - `@version`'s first argument must be of the form `\"name@X\"` or
              "`\"name@X > parent@Y\"`, where `name` has already been declared
              via `@schema`, and `parent@Y` has already been declared via `@version`.

              - `@version` declarations must list at least one required field,
              and must not list duplicate fields within the same declaration.
              """)
end

struct RequiredFieldInfo
    name::Symbol
    type::Union{Symbol,Expr}
    parameterize::Bool
    statement::Expr
end

Base.:(==)(a::RequiredFieldInfo, b::RequiredFieldInfo) = all(getfield(a, i) == getfield(b, i) for i in 1:fieldcount(RequiredFieldInfo))

function _parse_required_field_info!(f)
    f isa Symbol && (f = Expr(:(::), f, :Any))
    f.head == :(::) && (f = Expr(:(=), f, f.args[1]))
    f.head == :(=) && f.args[1] isa Symbol && (f.args[1] = Expr(:(::), f.args[1], :Any))
    f.head == :(=) && f.args[1].head == :(::) || error("couldn't normalize field expression: $f")
    type = f.args[1].args[2]
    parameterize = false
    if type isa Expr && type.head == :(<:)
        type = type.args[1]
        parameterize = true
    end
    return RequiredFieldInfo(f.args[1].args[1], type, parameterize, f)
end

function _has_valid_child_field_types(child_fields::NamedTuple, parent_fields::NamedTuple)
    for (name, child_type) in pairs(child_fields)
        if haskey(parent_fields, name)
            child_type <: parent_fields[name] || return false
        end
    end
    return true
end

_validate_wrt_parent(::NamedTuple, ::Nothing) = nothing

function _validate_wrt_parent(child_fields::NamedTuple, parent::SchemaVersion)
    declared(parent) || throw(SchemaVersionDeclarationError("parent schema cannot be used before it has been declared: $parent"))
    _has_valid_child_field_types(child_fields, required_fields(parent)) || throw(SchemaVersionDeclarationError("declared field types violate parent schema's field types"))
    return nothing
end

function _check_for_expected_field(schema::Tables.Schema, name::Symbol, ::Type{T}) where {T}
    i = findfirst(==(name), schema.names)
    if isnothing(i)
        Missing <: T || return missing
    else
        schema.types[i] <: T || return schema.types[i]
    end
    return nothing
end

function _generate_validation_definitions(schema_version::SchemaVersion)
    field_violation_check_statements = Expr[]
    for (fname, ftype) in pairs(required_fields(schema_version))
        fname = Base.Meta.quot(fname)
        push!(field_violation_check_statements, quote
            S = $Legolas.accepted_field_type(sv, $ftype)
            result = $Legolas._check_for_expected_field(ts, $fname, S)
            isnothing(result) || return $fname => result
        end)
    end
    return quote
        function $(Legolas).find_violation(ts::$(Tables).Schema, sv::$(Base.Meta.quot(typeof(schema_version))))
            $(field_violation_check_statements...)
            return nothing
        end
    end
end

function _record_type end # overloaded by `@version`
_record_type_name(prefix::Symbol, version::Integer) = Symbol(string(prefix, "V", version))
_schema_version_alias_name(prefix::Symbol, version::Integer) = Symbol(string(prefix, "SchemaV", version))

# Note also that this function's implementation is allowed to "observe" `Legolas.required_fields(parent)`
# (if a parent exists), but is NOT allowed to "observe" `Legolas.declaration(parent)`, since the latter
# includes the parent's declared field RHS statements. We cannot interpolate/incorporate these statements
# in the child's record type definition because they may reference bindings from the parent's `@version`
# callsite that are not available/valid at the child's `@version` callsite.
function _generate_record_type_definitions(schema_version::SchemaVersion, prefix::Symbol)
    # generate `schema_version_type_alias_definition`
    T = _schema_version_alias_name(prefix, version(schema_version))
    schema_version_type_alias_definition = :(const $T = $(Base.Meta.quot(typeof(schema_version))))

    # generate building blocks for record type definitions
    record_fields = required_fields(schema_version)
    _, declared_field_infos = declaration(schema_version)
    declared_field_infos = Dict(f.name => f for f in declared_field_infos)
    type_param_defs = Expr[]
    names_of_parameterized_fields = Symbol[]
    field_definitions = Expr[]
    field_assignments = Expr[]
    for (fname, ftype) in pairs(record_fields)
        fdef = :($fname::$(Base.Meta.quot(ftype)))
        info = get(declared_field_infos, fname, nothing)
        if !isnothing(info)
            fstmt = info.statement
            if info.parameterize
                T = Symbol("_", string(fname, "_T"))
                push!(type_param_defs, :($T <: $(info.type)))
                push!(names_of_parameterized_fields, fname)
                fdef = :($fname::$T)
                fstmt = :($fname = $(fstmt.args[2]))
            end
            push!(field_assignments, fstmt)
        end
        push!(field_definitions, fdef)
    end
    R = _record_type_name(prefix, version(schema_version))

    # generate `parent_record_application`
    field_kwargs = [Expr(:kw, n, :missing) for n in keys(record_fields)]
    parent_record_application = nothing
    parent = Legolas.parent(schema_version)
    if !isnothing(parent)
        p = gensym()
        P = Base.Meta.quot(_record_type(parent))
        parent_record_field_names = keys(required_fields(parent))
        parent_record_application = quote
            $p = $P(; $(parent_record_field_names...))
            $((:($n = $p.$n) for n in parent_record_field_names)...)
        end
    end

    # generate `inner_constructor_definitions` and `outer_constructor_definitions`
    kwargs_from_row = [Expr(:kw, n, :(get(row, $(Base.Meta.quot(n)), missing))) for n in keys(record_fields)]
    outer_constructor_definitions = :($R(row) = $R(; $(kwargs_from_row...)))
    if isempty(type_param_defs)
        inner_constructor_definitions = quote
            function $R(; $(field_kwargs...))
                $parent_record_application
                $(field_assignments...)
                return new($(keys(record_fields)...))
            end
        end
    else
        type_param_names = [p.args[1] for p in type_param_defs]
        inner_constructor_definitions = quote
            function $R{$(type_param_names...)}(; $(field_kwargs...)) where {$(type_param_names...)}
                $parent_record_application
                $(field_assignments...)
                return new{$(type_param_names...)}($(keys(record_fields)...))
            end
            function $R(; $(field_kwargs...))
                $parent_record_application
                $(field_assignments...)
                return new{$((:(typeof($n)) for n in names_of_parameterized_fields)...)}($(keys(record_fields)...))
            end
        end
        outer_constructor_definitions = quote
            $outer_constructor_definitions
            $R{$(type_param_names...)}(row) where {$(type_param_names...)} = $R{$(type_param_names...)}(; $(kwargs_from_row...))
        end
    end

    # generate `base_overload_definitions`
    equal_rhs_statement = foldr((x, y) -> :($x && $y), (:(a.$f == b.$f) for f in keys(record_fields)))
    isequal_rhs_statement = foldr((x, y) -> :($x && $y), (:(isequal(a.$f, b.$f)) for f in keys(record_fields)))
    hash_rhs_statement = foldr((x, y) -> :(hash($x, $y)), (:(r.$f) for f in keys(record_fields)); init=:h)
    base_overload_definitions = quote
        Base.:(==)(a::$R, b::$R) = $equal_rhs_statement
        Base.isequal(a::$R, b::$R) = $isequal_rhs_statement
        Base.hash(r::$R, h::UInt) = hash($R, $hash_rhs_statement)
        Base.NamedTuple(r::$R) = (; $((:(r.$f) for f in keys(record_fields))...))
    end

    # generate `arrow_overload_definitions`
    record_type_arrow_name = Base.Meta.quot(Symbol("JuliaLang.Legolas.Generated.$R"))
    arrow_overload_definitions = quote
        $Arrow.ArrowTypes.arrowname(::Type{<:$R}) = $record_type_arrow_name
        $Arrow.ArrowTypes.ArrowType(::Type{R}) where {R<:$R} = NamedTuple{fieldnames(R),Tuple{fieldtypes(R)...}}
        $Arrow.ArrowTypes.toarrow(r::$R) = NamedTuple(r)
        $Arrow.ArrowTypes.JuliaType(::Val{$record_type_arrow_name}, ::Any) = $R
        $Arrow.ArrowTypes.fromarrow(::Type{<:$R}, $(keys(record_fields)...)) = $R(; $(keys(record_fields)...))
    end

    return quote
        $schema_version_type_alias_definition
        struct $R{$(type_param_defs...)} <: $Legolas.AbstractRecord
            $(field_definitions...)
            $inner_constructor_definitions
        end
        $outer_constructor_definitions
        $base_overload_definitions
        $arrow_overload_definitions
    end
end

"""
    @version "name@version" begin
        required_field_expression_1
        required_field_expression_2
        ⋮
    end

    @version "name@version > parent_name@parent_version" begin
        required_field_expression_1
        required_field_expression_2
        ⋮
    end

Declare a new `Legolas.SchemaVersion{name,version}` and overload relevant Legolas method definitions
with schema-specific behaviors in accordance with the declared schema version identifier, parent
identifier (if present), and required fields.

Each element of `field_expressions` defines a required field for the declared schema, and is an expression
of the form `field::F = rhs` where:

- `field` is the corresponding field's name
- `::F` denotes the field's type constraint (if elided, defaults to `::Any`).
- `rhs` is the expression which produces `field::F` (if elided, defaults to `field`).

Accounting for all of the aforementioned allowed elisions, possible valid forms of elements of
`field_expressions` include:

- `field::F = rhs`
- `field::F` (interpreted as `field::F = field`)
- `field = rhs` (interpreted as `field::Any = rhs`)
- `field` (interpreted as `field::Any = field`)

`F` is generally a type literal, but may also be an expression of the form `(<:T)`, in which case
the declared schema version's generated record type will expose a type parameter (constrained to be
a subtype of `T`) for the given field. For example:

    julia> @schema "example.foo" Foo

    julia> @version "example.foo@1" begin
            x::Int
            y::(<:Real)
        end

    julia> FooV1(x=1, y=2.0)
    FooV1{Float64}: (x = 1, y = 2.0)

    julia> FooV1{Float32}(x=1, y=2)
    FooV1{Float32}: (x = 1, y = 2.0f0)

    julia> FooV1(x=1, y="bad")
    ERROR: TypeError: in FooV1, in _y_T, expected _y_T<:Real, got Type{String}

Note that:

- A `Legolas.SchemaVersionDeclarationError` will be thrown if:
    - The provided schema version identifier is invalid or is not a string literal
    - The identified schema has not yet been declared via `@schema`
    - If there are no required field expressions, duplicate required fields are declared, or a given
      expression is invalid
    - (if a parent schema is specified) This schema declaration does not comply with its parent's
      schema declaration, or the parent hasn't yet been declared
- This macro expects to be evaluated within top-level scope.

For more details and examples, please see `Legolas.jl/examples/tour.jl` and the
"Schema-Related Concepts/Conventions" section of the Legolas.jl documentation.
"""
macro version(id, required_field_statements)
    id isa String || return :(throw(SchemaVersionDeclarationError("schema version identifier must be a string literal")))

    # parse `id`
    schema_versions = nothing
    try
        schema_versions = parse_identifier(id)
    catch err
        msg = "Error encountered attempting to parse schema version identifier.\n" *
              "Received: \"$id\"\n" *
              "Encountered: " * sprint(showerror, err)
        return :(throw(SchemaVersionDeclarationError($msg)))
    end
    if length(schema_versions) == 1
        schema_version, parent = first(schema_versions), nothing
    elseif length(schema_versions) == 2
        schema_version, parent = schema_versions[1], schema_versions[2]
        name(schema_version) == name(parent) && return :(throw(SchemaVersionDeclarationError("cannot extend from a different version of the same schema")))
    else
        return :(throw(SchemaVersionDeclarationError(string("schema version identifier should specify at most one parent, found multiple: ", $schema_versions))))
    end

    # parse `required_field_statements`
    if !(required_field_statements isa Expr && required_field_statements.head == :block && !isempty(required_field_statements.args))
        return :(throw(SchemaVersionDeclarationError("malformed or missing declaration of required fields")))
    end
    required_field_statements = [f for f in required_field_statements.args if !(f isa LineNumberNode)]
    required_field_infos = RequiredFieldInfo[]
    for stmt in required_field_statements
        original_stmt = Base.Meta.quot(deepcopy(stmt))
        try
            push!(required_field_infos, _parse_required_field_info!(stmt))
        catch
            return :(throw(SchemaVersionDeclarationError(string("malformed `@version` field expression: ", $original_stmt))))
        end
    end
    if !allunique(f.name for f in required_field_infos)
        msg = string("cannot have duplicate field names in `@version` declaration; recieved: ", [f.name for f in required_field_infos])
        return :(throw(SchemaVersionDeclarationError($msg)))
    end
    field_names_types = Expr(:tuple, (:($(f.name) = $(esc(f.type))) for f in required_field_infos)...)

    # basic definition elements
    quoted_schema_version = Base.Meta.quot(schema_version)
    quoted_schema_version_type = Base.Meta.quot(typeof(schema_version))
    quoted_parent = Base.Meta.quot(parent)
    type_prefix = schema_type_prefix(Val(name(schema_version)))
    isnothing(type_prefix) && return :(throw(SchemaVersionDeclarationError(string($(name(schema_version)), "must be declared via `@schema` before its initial `@version` declaration"))))

    # basic accessor function definitions
    full_identifier_string = string(name(schema_version), '@', version(schema_version))
    child_identifier_string = full_identifier_string
    required_field_names_types = field_names_types
    if !isnothing(parent)
        full_identifier_string = :(string($full_identifier_string, '>', Legolas.identifier($quoted_parent)))
        child_identifier_string = string(child_identifier_string, '>', name(parent), '@', version(parent))
        required_field_names_types = :(merge(Legolas.required_fields($quoted_parent), $required_field_names_types))
    end
    schema_version_declaration = :($child_identifier_string => copy($(Base.Meta.quot(required_field_infos))))
    check_against_declaration = :($child_identifier_string => $(Base.Meta.quot(required_field_infos)))

    return quote
        if Legolas.declared($quoted_schema_version) && Legolas.declaration($quoted_schema_version) != $check_against_declaration
            throw(SchemaVersionDeclarationError("invalid redeclaration of existing schema; all `@schema` redeclarations must exactly match previous declarations"))
        else
            Legolas._validate_wrt_parent($field_names_types, $quoted_parent)

            @inline Legolas.declared(::$quoted_schema_version_type) = true

            @inline Legolas.identifier(::$quoted_schema_version_type) = $full_identifier_string

            @inline Legolas.parent(::$quoted_schema_version_type) = $quoted_parent

            Legolas.required_fields(::$quoted_schema_version_type) = $required_field_names_types

            Legolas.declaration(::$quoted_schema_version_type) = $schema_version_declaration

            $(esc(:eval))(Legolas._generate_validation_definitions($quoted_schema_version))

            $(esc(:eval))(Legolas._generate_record_type_definitions($quoted_schema_version, $(Base.Meta.quot(type_prefix))))

            Legolas._record_type(::$quoted_schema_version_type) = $(esc(_record_type_name(type_prefix, version(schema_version))))
        end
        nothing
    end
end
