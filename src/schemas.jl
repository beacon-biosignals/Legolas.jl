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
##### `Schema`
#####

"""
    Legolas.Schema{name,version}

A type representing a Legolas schema. The `name` (a `Symbol`) and `version` (an `Integer`)
are surfaced as type parameters, allowing them to be utilized for dispatch.

For more details and examples, please see `Legolas.jl/examples/tour.jl` and the
"Schema-Related Concepts/Conventions" section of the Legolas.jl documentation.

Throws an `ArgumentError` if `version` is not non-negative; prefer the constructor


Schema(name, version)

which performs additional validation on construction.

See also: [`schema_name`](@ref), [`schema_version`](@ref), [`schema_parent`](@ref)
"""
struct Schema{name,version}
    function Schema{name,version}() where {name,version}
        version isa Integer && version >= 0 || throw(ArgumentError("`version` in `Schema{_,version}` must be a non-negative integer, received: $version"))
        return new{name,version}()
    end
end

"""
    Legolas.Schema(name::AbstractString, version::Integer)

Return `Legolas.Schema{Symbol(name),version}()`.

Throws an `ArgumentError` if `name` is not a valid schema name.

Prefer using this constructor over `Legolas.Schema{Symbol(name),version}()` directly.
"""
function Schema(name::AbstractString, version::Integer)
    is_valid_schema_name(name) || throw(ArgumentError("argument is not a valid `Legolas.Schema` name: \"$name\""))
    return Schema{Symbol(name),version}()
end

Schema(schema::Schema) = schema

#####
##### `parse_schema_identifier`
#####

"""
TODO

"""
function parse_schema_identifier(x::AbstractString)
    name_and_version_per_schema = [split(strip(y), '@') for y in split(x, '>')]
    schemas = Schema[]
    invalid = isempty(name_and_version_per_schema)
    if !invalid
        for nv in name_and_version_per_schema
            if length(nv) != 2
                invalid = true
                break
            end
            name, version = nv
            version = tryparse(Int, version)
            version isa Int && push!(schemas, Schema(name, version))
        end
    end
    (invalid || isempty(schemas)) && throw(ArgumentError("failed to parse seemingly invalid/malformed schema identifier string: \"$x\""))
    return schemas
end

#####
##### `@alias`
#####

"""
    Legolas.@alias(schema_name, T)

Define a convenience type alias of the form:

    const T{v} = Legolas.Schema{Symbol(schema_name),v}

...along with some default behaviors that encourage proper usage by downstream callers,
especially the inclusion of the schema version integer `v` in every direct invocation.
"""
macro alias(schema_name, T)
    schema_name isa String || return :(throw(ArgumentError("`schema_name` provided to `@alias` must be a string literal")))
    occursin('@', schema_name) && return :(throw(ArgumentError("`schema_name` provided to `@alias` should not include an `@` version clause")))
    is_valid_schema_name(schema_name) || return :(throw(ArgumentError("`schema_name` provided to `@alias` is not a valid `Legolas.Schema` name: \"" * $schema_name * "\"")))
    schema_symbol = Base.Meta.quot(Symbol(schema_name))
    version_symbol = esc(:v)
    quoted_T = Base.Meta.quot(T)
    T = esc(T)
    return quote
        const $T{$version_symbol} = Legolas.Schema{$schema_symbol,$version_symbol}
        @inline $T(v::Integer) = Legolas.Schema{$schema_symbol,v}()
        $T() = error("invocations of `", $quoted_T, "` must specify a schema version integer; instead of `", $quoted_T,
                     "()`, try `", $quoted_T, "(v)` where `v` is an appropriate schema version integer")
    end
end

#####
##### `UnknownSchemaError`
#####

struct UnknownSchemaError <: Exception
    schema::Legolas.Schema
end

function Base.showerror(io::IO, e::UnknownSchemaError)
    print(io, """
              UnknownSchemaError: encountered unknown `Legolas.Schema` type: $(e.schema)

              This generally indicates that this schema has not been declared (i.e.
              the schema's corresponding `@schema` statement has not been executed) in
              the current Julia session.

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
##### `Schema` accessors/generators
#####

"""
    schema_name(::Legolas.Schema{name})

Return `name`.
"""
@inline schema_name(::Schema{name}) where {name} = name

"""
    schema_version(::Legolas.Schema{name,version})

Return `version`.
"""
@inline schema_version(::Schema{name,version}) where {name,version} = version

"""
    schema_parent(::Legolas.Schema)

Return the `Legolas.Schema` instance that corresponds to the parent of the given `Legolas.Schema`.
"""
@inline schema_parent(::Schema) = nothing

"""
    schema_declared(schema::Legolas.Schema{name,version})

Return `true` if the schema `name@version` has been declared via `@schema` in the current Julia
session; return `false` otherwise.
"""
@inline schema_declared(::Schema) = false

"""
    schema_identifier(::Legolas.Schema)

Return this `Legolas.Schema`'s fully qualified schema identifier. This string is serialized
as the `\"$LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY\"` field value in table metadata for table
written via [`Legolas.write`](@ref).
"""
schema_identifier(schema::Schema) = throw(UnknownSchemaError(schema))

"""
    schema_fields(schema::Legolas.Schema)

Return a `NamedTuple{...,Tuple{Vararg{DataType}}` whose fields take the form:

    <name of field required by schema> = <field's type>

The returned fields include those inherited from `schema_parent(schema)` (if
`schema` has a parent).
"""
schema_fields(schema::Schema) = throw(UnknownSchemaError(schema))

"""
    schema_declaration(schema::Legolas.Schema)

Return

- returns a `Pair{String,Dict{Symbol,Expr}}` containing each required field statement for interactive discovery usage (NOT including parent fields)
- does NOT include parent fields
- should NOT be used for anything other than interactive discovery
"""
schema_declaration(schema::Schema) = throw(UnknownSchemaError(schema))

#####
##### `Schema` printing
#####

Base.show(io::IO, schema::Schema) = print(io, "Schema(\"$(schema_name(schema))\", $(schema_version(schema)))")

#####
##### `Schema` Arrow (de)serialization
#####

# support (de)serialization of Schemas to Arrow
const LEGOLAS_SCHEMA_ARROW_NAME = Symbol("JuliaLang.Legolas.Schema")
Arrow.ArrowTypes.arrowname(::Type{<:Schema}) = LEGOLAS_SCHEMA_ARROW_NAME
Arrow.ArrowTypes.ArrowType(::Type{<:Schema}) = String
Arrow.ArrowTypes.toarrow(schema::Schema) = schema_identifier(schema)
Arrow.ArrowTypes.JuliaType(::Val{LEGOLAS_SCHEMA_ARROW_NAME}, ::Any) = Schema
Arrow.ArrowTypes.fromarrow(::Type{<:Schema}, qualified_string) = Schema(qualified_string)

#####
##### `Tables.Schema` validation
#####

"""
    Legolas.find_violation(tables_schema::Tables.Schema, legolas_schema::Legolas.Schema)

TODO

- every non-`>:Missing` field required by `legolas_schema` is present in `tables_schema`.
- `T <: S` for each field `f::T` in `tables_schema` that matches a required `legolas_schema` field `f::S`.
"""
find_violation(::Tables.Schema, schema::Schema) = throw(UnknownSchemaError(schema))

function _find_violation end

"""
    Legolas.validate(tables_schema::Tables.Schema, legolas_schema::Legolas.Schema)

TODO

Throws an `ArgumentError` if ____, otherwise returns `nothing`.
"""
function validate(tables_schema::Tables.Schema, legolas_schema::Schema)
    result = find_violation(tables_schema, legolas_schema)
    isnothing(result) && return nothing
    field, violation = result
    ismissing(violation) && throw(ArgumentError("could not find expected field `$field` in $tables_schema"))
    expected = getfield(schema_fields(legolas_schema), field)
    throw(ArgumentError("field `$field` has unexpected type; expected <:$expected, found $violation"))
end

complies_with(tables_schema::Tables.Schema, legolas_schema::Schema) = isnothing(find_violation(tables_schema, legolas_schema))

#####
##### `row`
#####
# TODO more heavily encourage idempotency in documentation/examples

row(schema::Schema; fields...) = throw(UnknownSchemaError(schema))
row(schema::Schema, fields::NamedTuple) = row(schema; fields...)
row(schema::Schema, fields) = row(schema, NamedTuple(Tables.Row(fields)))

function _row end

#####
##### `@schema`
#####

struct SchemaDeclarationError <: Exception
    message::String
end

function Base.showerror(io::IO, e::SchemaDeclarationError)
    print(io, """
              SchemaDeclarationError: $(e.message)

              Note that valid `@schema` declarations meet these expecations:

              - `@schema`'s first argument must be of the form `\"name@X\"` or
              "`\"name@X > parent@Y\"`, where `name` and `parent` are valid
              "Legolas schema names.

              - `@schema` declarations must list at least one required field,
              and must not list duplicate fields within the same declaration.
              """)
end

function _normalize_field(f)
    original_f = f
    f isa Symbol && (f = Expr(:(::), f, :Any))
    f.head == :(::) && (f = Expr(:(=), f, f.args[1]))
    f.head == :(=) && f.args[1] isa Symbol && (f.args[1] = Expr(:(::), f.args[1], :Any))
    f.head == :(=) && f.args[1].head == :(::) || throw(SchemaDeclarationError("malformed `@schema` field expression: $original_f"))
    return f
end

function _has_valid_child_field_types(child_fields::NamedTuple, parent_fields::NamedTuple)
    for (name, child_type) in pairs(child_fields)
        if haskey(parent_fields, name)
            child_type <: parent_fields[name] || return false
        end
    end
    return true
end

_validate_wrt_parent(child_fields::NamedTuple, parent::Nothing) = nothing

function _validate_wrt_parent(child_fields::NamedTuple, parent::Schema)
    schema_declared(parent) || throw(SchemaDeclarationError("parent schema cannot be used before it has been declared: $parent"))
    _has_valid_child_field_types(child_fields, schema_fields(parent)) || throw(SchemaDeclarationError("declared field types violate parent schema's field types"))
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

# Note that there exists a clean generic approach for implementing `row` that is applicable
# to the other Legolas functions that similarly compose child/parent schema behaviors:
#
#    function row(schema::Schema; fields...)
#        parent = schema_parent(schema)
#        parent isa Schema && (fields = row(parent; fields...))
#        return _row(schema; fields...)
#    end
#
# However, basic benchmarking as of Julia 1.6 demonstrates that the additional `parent isa Schema`
# branch in the above approach can induce unnecessary allocations for schemas with a few ancestors,
# while the approach below (which "hardcodes" the known result of this branch at definition time)
# does not.
#
# Note also that we cannot just interpolate the parent's declared field statements directly
# into the child's `row` definition, since the parent's field statements may reference bindings
# from the parent's declaration site that are not available/valid at the child's declaration
# site.

macro schema(id, field_exprs...)
    id isa String || return :(throw(SchemaDeclarationError("schema identifier must be a string literal")))
    schemas = nothing
    try
        schemas = parse_schema_identifier(id)
    catch err
        msg = "Error encountered attempting to parse schema identifier.\n" *
              "Received: \"$id\"\n" *
              "Encountered: " * sprint(showerror, err)
        return :(throw(SchemaDeclarationError($msg)))
    end
    if length(schemas) == 1
        schema, parent = first(schemas), nothing
    elseif length(schemas) == 2
        schema, parent = schemas[1], schemas[2]
    else
        return :(throw(SchemaDeclarationError(string("schema identifier should specify at most one parent, found multiple: ", $schemas))))
    end

    isempty(field_exprs) && return :(throw(SchemaDeclarationError("no required fields declared")))
    fields = [(name=stmt.args[1].args[1], type=stmt.args[1].args[2], statement=stmt)
              for stmt in map(_normalize_field, field_exprs)]
    allunique(f.name for f in fields) || return :(throw(SchemaDeclarationError(string("cannot have duplicate field names in `@schema` declaration; recieved: ", $([f.name for f in fields])))))
    field_names_types = Expr(:tuple, (:($(f.name) = $(f.type)) for f in fields)...)

    quoted_schema = Base.Meta.quot(schema)
    quoted_schema_type = Base.Meta.quot(typeof(schema))
    quoted_parent = Base.Meta.quot(parent)

    qualified_string = string(schema_name(schema), '@', schema_version(schema))
    declared_string = qualified_string
    total_schema_fields = field_names_types
    parent_row_invocation = nothing
    parent_find_violation_invocation = nothing
    if !isnothing(parent)
        qualified_string = :(string($qualified_string, '>', Legolas.schema_identifier($quoted_parent)))
        declared_string = string(declared_string, '>', schema_name(parent), '@', schema_version(parent))
        total_schema_fields = :(merge(Legolas.schema_fields($quoted_parent), $total_schema_fields))
        parent_row_invocation = :(fields = Legolas.row($quoted_parent; fields...))
        parent_find_violation_invocation = quote
            result = Legolas.find_violation(tables_schema, $quoted_parent)
            isnothing(result) || return result
        end
    end

    quoted_schema_declaration = Base.Meta.quot(declared_string => Dict(f.name => f.statement for f in fields))
    check_for_expected_field_statements = map(fields) do f
        fname = Base.Meta.quot(f.name)
        return quote
            result = Legolas._check_for_expected_field(tables_schema, $fname, $(esc(f.type)))
            isnothing(result) || return $fname => result
        end
    end
    return quote
        if Legolas.schema_declared($quoted_schema) && Legolas.schema_declaration($quoted_schema) != $quoted_schema_declaration
            throw(SchemaDeclarationError("invalid redeclaration of existing schema; all `@schema` redeclarations must exactly match previous declarations"))
        else
            Legolas._validate_wrt_parent($field_names_types, $quoted_parent)

            @inline Legolas.schema_declared(::$quoted_schema_type) = true

            @inline Legolas.schema_identifier(::$quoted_schema_type) = $qualified_string

            @inline Legolas.schema_parent(::$quoted_schema_type) = $quoted_parent

            Legolas.schema_fields(::$quoted_schema_type) = $total_schema_fields

            Legolas.schema_declaration(::$quoted_schema_type) = $quoted_schema_declaration

            function Legolas._row(::$quoted_schema_type; $((Expr(:kw, esc(f.name), :missing) for f in fields)...), extra...)
                $((esc(f.statement) for f in fields)...)
                return (; $((Expr(:kw, esc(f.name), esc(f.name)) for f in fields)...), extra...)
            end

            function Legolas.row(schema::$quoted_schema_type; fields...)
                $parent_row_invocation
                return Legolas._row(schema; fields...)
            end

            function Legolas._find_violation(tables_schema::Tables.Schema, legolas_schema::$quoted_schema_type)
                $(check_for_expected_field_statements...)
                return nothing
            end

            function Legolas.find_violation(tables_schema::Tables.Schema, legolas_schema::$quoted_schema_type)
                $parent_find_violation_invocation
                return Legolas._find_violation(tables_schema, legolas_schema)
            end
        end
        nothing
    end
end


