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

Return a `Pair{String,Vector{Pair{Symbol,NamedTuple}}}` of the form

    schema_identifier_used_for_declaration::String => required_fields::Vector{Pair{Symbol,NamedTuple}}

where each key-value pair in `required_fields` takes the form:

    TODO the RHS here should actually be the `NamedTuple`s emitted by `_normalize_field!`
    required_field_name::Symbol => normalized_required_field_assignment_statement::Expr

Note that `declaration` is primarily intended to be used for interactive discovery purposes, and
does not include the contents of `declaration(parent(sv))`.
"""
declaration(sv::SchemaVersion) = throw(UnknownSchemaError(sv))

#####
##### `Tables.Schema` validation
#####

accepted_field_type(::SchemaVersion, T::DataType) = T
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
    expected = getfield(schema_fields(sv), field)
    throw(ArgumentError("field `$field` has unexpected type; expected <:$expected, found $violation"))
end

"""
    Legolas.complies_with(ts::Tables.Schema, sv::Legolas.SchemaVersion)

Return `isnothing(find_violation(ts, sv))`.

See also: [`Legolas.find_violation`](@ref), [`Legolas.validate`](@ref)
"""
complies_with(ts::Tables.Schema, sv::SchemaVersion) = isnothing(find_violation(ts, sv))

#####
##### `@schema`
#####

"""
    TODO
"""
schema_type_prefix(::Val{n}) where {n} = throw(UnknownSchemaError(n, nothing))

macro schema(schema_name, schema_prefix)
    schema_name isa String || return :(throw(ArgumentError("`schema_name` provided to `@schema` must be a string literal")))
    occursin('@', schema_name) && return :(throw(ArgumentError("`schema_name` provided to `@schema` should not include an `@` version clause")))
    is_valid_schema_name(schema_name) || return :(throw(ArgumentError("`schema_name` provided to `@schema` is not a valid `Legolas.SchemaVersion` name: \"" * $schema_name * "\"")))
    schema_prefix isa Symbol || return :(throw(ArgumentError("TODO")))
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
              "`\"name@X > parent@Y\"`, where `name` and `parent` are valid
              Legolas schema names.

              - `@version` declarations must list at least one required field,
              and must not list duplicate fields within the same declaration.
              """)
end

function _normalize_field!(f)
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
    return (name=f.args[1].args[1], type=type, parameterize=parameterize, statement=f)
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
    declared(parent) || throw(SchemaDeclarationError("parent schema cannot be used before it has been declared: $parent"))
    _has_valid_child_field_types(child_fields, required_fields(parent)) || throw(SchemaDeclarationError("declared field types violate parent schema's field types"))
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

macro version(id, required_fields)
    id isa String || return :(throw(SchemaVersionDeclarationError("schema version identifier must be a string literal")))

    schemas = nothing
    try
        schemas = parse_identifier(id)
    catch err
        msg = "Error encountered attempting to parse schema version identifier.\n" *
              "Received: \"$id\"\n" *
              "Encountered: " * sprint(showerror, err)
        return :(throw(SchemaVersionDeclarationError($msg)))
    end
    if length(schemas) == 1
        schema, parent = first(schemas), nothing
    elseif length(schemas) == 2
        schema, parent = schemas[1], schemas[2]
    else
        return :(throw(SchemaVersionDeclarationError(string("schema version identifier should specify at most one parent, found multiple: ", $schemas))))
    end

    required_fields isa Expr && required_fields.head == :block && !isempty(required_fields.args) || return :(throw(SchemaVersionDeclarationError("malformed or missing declaration of required fields")))
    required_fields = [f for f in required_fields.args if !(f isa LineNumberNode)]
    fields = NamedTuple[]
    for stmt in required_fields
        original_stmt = Base.Meta.quot(deepcopy(stmt))
        try
            push!(fields, _normalize_field!(stmt))
        catch
            return :(throw(SchemaVersionDeclarationError(string("malformed `@schema` field expression: ", $original_stmt))))
        end
    end
    allunique(f.name for f in fields) || return :(throw(SchemaVersionDeclarationError(string("cannot have duplicate field names in `@schema` declaration; recieved: ", $([f.name for f in fields])))))
    field_names_types = Expr(:tuple, (:($(f.name) = $(esc(f.type))) for f in fields)...)

    quoted_schema = Base.Meta.quot(schema)
    quoted_schema_type = Base.Meta.quot(typeof(schema))
    quoted_parent = Base.Meta.quot(parent)

    qualified_string = string(name(schema), '@', version(schema))
    declared_string = qualified_string
    total_schema_fields = field_names_types
    parent_row_invocation = nothing
    parent_find_violation_invocation = nothing
    if !isnothing(parent)
        qualified_string = :(string($qualified_string, '>', Legolas.identifier($quoted_parent)))
        declared_string = string(declared_string, '>', name(parent), '@', version(parent))
        total_schema_fields = :(merge(Legolas.fields($quoted_parent), $total_schema_fields))
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
        if Legolas.declared($quoted_schema) && Legolas.declaration($quoted_schema) != $quoted_schema_declaration
            throw(SchemaVersionDeclarationError("invalid redeclaration of existing schema; all `@schema` redeclarations must exactly match previous declarations"))
        else
            Legolas._validate_wrt_parent($field_names_types, $quoted_parent)

            @inline Legolas.declared(::$quoted_schema_type) = true

            @inline Legolas.identifier(::$quoted_schema_type) = $qualified_string

            @inline Legolas.parent(::$quoted_schema_type) = $quoted_parent

            Legolas.fields(::$quoted_schema_type) = $total_schema_fields

            Legolas.declaration(::$quoted_schema_type) = $quoted_schema_declaration

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

# _, parent_fields = declaration(parent)
# fields = collect(NamedTuple, values(merge(NamedTuple(parent_fields), declared_fields)))

function _generate_record_definition(prefix::Symbol, v::Int, fields)
    params = Expr[]
    parameterized_fields = Symbol[]
    field_definitions = Expr[]
    field_assignments = Expr[]
    for f in fields
        if f.parameterize
            T = Symbol("_", string(f.name, "_T"))
            push!(params, :($T <: $(f.type)))
            f = merge(f, (; type=T, statement=:($(f.name) = $(f.statement.args[2]))))
            push!(parameterized_fields, f.name)
        end
        push!(field_definitions, :($(f.name)::$(f.type)))
        push!(field_assignments, f.statement)
    end
    R = Symbol(string(prefix, "V", v))
    param_names = [p.args[1] for p in params]
    row = gensym()
    row_fields = [:($row.$(f.name)) for f in fields]
    row_params = [:(typeof($row.$f)) for f in parameterized_fields]
    field_params = [:(typeof($f)) for f in parameterized_fields]
    field_kwargs = [Expr(:kw, f.name, :missing) for f in fields]
    return quote
        struct $R{$(params...)} <: Tables.AbstractRow
            $(field_definitions...)
            function $R{$(param_names...)}(; $(field_kwargs...)) where {$(param_names...)}
                $(field_assignments...)
                return new{$(param_names...)}($((f.name for f in fields)...))
            end
        end
        $R(; $(field_kwargs...)) = $R{$(field_params...)}(; $((f.name for f in fields)...))
        $R($row) = $R{$(row_params...)}(; $(row_fields...))
        $R{$(param_names...)}($row) where {$(param_names...)} = $R{$(param_names...)}(; $(row_fields...))
        @inline Tables.getcolumn(r::$R, i::Int) = getfield(r, i)
        @inline Tables.getcolumn(r::$R, nm::Symbol) = getfield(r, nm)
        @inline Tables.columnnames(r::$R) = fieldnames(typeof(r))
    end
end
