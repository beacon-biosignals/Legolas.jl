#####
##### Tables.jl operations/utilities
#####

function _columns(table)
    try
        return Tables.columns(table)
    catch
        @warn "encountered error during internal invocation of `Tables.columns`; provided table might not be Tables.jl-compliant"
        rethrow()
    end
end

"""
    locations(collections::Tuple)

Return a `Dict` whose keys are the set of all elements across all provided collections,
and whose values are the indices that locate each corresponding element across all
provided collecitons.

Specifically, `locations(collections)[k][i]` will return a `Vector{Int}` whose elements
are the index locations of `k` in `collections[i]`. If `!(k in collections[i])`, this
`Vector{Int}` will be empty.

For example:

```
julia> Legolas.locations((['a', 'b', 'c', 'f', 'b'],
                          ['d', 'c', 'e', 'b'],
                          ['f', 'a', 'f']))
Dict{Char, Tuple{Vector{Int64}, Vector{Int64}, Vector{Int64}}} with 6 entries:
  'f' => ([4], [], [1, 3])
  'a' => ([1], [], [2])
  'c' => ([3], [2], [])
  'd' => ([], [1], [])
  'e' => ([], [3], [])
  'b' => ([2, 5], [4], [])
```

This function is useful as a building block for higher-level tabular operations
that require indexing/grouping along specific sets of elements.
"""
function locations(collections::T) where {T<:Tuple}
    N = fieldcount(T)
    K = promote_type(eltype.(collections)...)
    results = Dict{K,NTuple{N,Vector{Int}}}()
    for (c, collection) in enumerate(collections)
        for (i, item) in enumerate(collection)
            push!(get!(() -> ntuple(_ -> Int[], N), results, item)[c], i)
        end
    end
    return results
end

function _iterator_for_column(table, c)
    Tables.columnaccess(table) && return Tables.getcolumn(_columns(table), c)
    # there's not really a need to actually materialize this iterable
    # for the caller, but doing so allows the caller to more usefully
    # employ `eltype` on this function's output (since e.g. a generator
    # would just return `Any` for the eltype)
    return [Tables.getcolumn(r, c) for r in Tables.rows(table)]
end

"""
    Legolas.gather(column_name, tables...; extract=((table, idxs) -> view(table, idxs, :)))

Gather rows from `tables` into a unified cross-table index along `column_name`. Returns
a `Dict` whose keys are the unique values of `column_name` across `tables`, and whose
values are tuples of the form:

    (rows_matching_key_in_table_1, rows_matching_key_in_table_2, ...)

The provided `extract` function is used to extract rows from each table; it takes
as input a table and a `Vector{Int}` of row indices, and returns the corresponding
subtable. The default definition is sufficient for `DataFrames` tables.

Note that this function may internally call `Tables.columns` on each input table, so
it may be slower and/or require more memory if `any(!Tables.columnaccess, tables)`.

Note that we intend to eventually migrate this function from Legolas.jl to a more appropriate package.
"""
function gather(column_name, tables::Vararg{Any,N};
                extract=((cols, idxs) -> view(cols, idxs, :))) where {N}
    iters = ntuple(i -> _iterator_for_column(tables[i], column_name), N)
    return Dict(id => ntuple(i -> extract(tables[i], locs[i]), N) for (id, locs) in locations(iters))
end

"""
    Legolas.materialize(table)

Return a fully deserialized copy of `table`.

This function is useful when `table` has built-in deserialize-on-access or
conversion-on-access behavior (like `Arrow.Table`) and you'd like to pay
such access costs upfront before repeatedly accessing the table.

Note that we intend to eventually migrate this function from Legolas.jl to a more appropriate package.
"""
materialize(table) = map(collect, Tables.columntable(table))

#####
##### read/write Arrow content to generic path types
#####
# It would be better if Arrow.jl supported a generic API for nonstandard path-like types so that
# we can avoid potential intermediate copies here, but its documentation is explicit that it only
# supports `Union{IO,String}`.
#
# TODO: upstream improvements to Arrow.jl to obviate these?

write_full_path(path::AbstractString, bytes) = (mkpath(dirname(path)); Base.write(path, bytes))
write_full_path(path, bytes) = Base.write(path, bytes)

read_arrow(io_or_path::Union{IO,String,Vector{UInt8}}) = Arrow.Table(io_or_path)
read_arrow(path) = read_arrow(Base.read(path))

write_arrow(path::String, table; kwargs...) = Arrow.write(path, table; kwargs...)
write_arrow(io::IO, table; kwargs...) = Arrow.write(io, table; file=get(kwargs, :file, true), kwargs...)
write_arrow(path, table; kwargs...) = (io = IOBuffer(); write_arrow(io, table; kwargs...); write_full_path(path, take!(io)))

#####
##### `extract_schema_version`
#####

"""
    Legolas.extract_schema_version(table)

Attempt to extract Arrow metadata from `table` via `Arrow.getmetadata(table)`.

If Arrow metadata is present and contains `\"$LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY\" => s`,
return `first(parse_identifier(s))`

Otherwise, return `nothing`.
"""
function extract_schema_version(table)
    v = extract_metadata(table, LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY)
    isnothing(v) && return nothing
    return first(parse_identifier(v))
end

function extract_metadata(table, key)
    metadata = Arrow.getmetadata(table)
    if !isnothing(metadata)
        for (k, v) in metadata
            k == key && return v
        end
    end
    return nothing
end

#####
##### `read`/`write`
#####

"""
    Legolas.read(io_or_path; validate::Bool=true)

Read and return an `Arrow.Table` from `io_or_path`.

If `validate` is `true`, `Legolas.read` will attempt to extract a `Legolas.SchemaVersion` from
the deserialized `Arrow.Table`'s metadata and use `Legolas.validate` to verify that the table's
`Table.Schema` complies with the extracted `Legolas.SchemaVersion` before returning the table.

Note that `io_or_path` may be any type that supports `Base.read(io_or_path)::Vector{UInt8}`.
"""
function read(io_or_path; validate::Bool=true)
    table = read_arrow(io_or_path)
    if validate
        sv = extract_schema_version(table)
        isnothing(sv) && throw(ArgumentError("""
                                             could not extract valid `Legolas.SchemaVersion` from the `Arrow.Table` read
                                             via `Legolas.read`; is it missing the expected custom metadata and/or the
                                             expected \"$LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY\" field?
                                             """))

        provider_name = lift(Symbol, extract_metadata(table, LEGOLAS_SCHEMA_PROVIDER_NAME_METADATA_KEY))
        provider_version = lift(VersionNumber, extract_metadata(table, LEGOLAS_SCHEMA_PROVIDER_VERSION_METADATA_KEY))
        # If we don't have the schema declared in our session,
        # then throw an error with all the information we have available about where
        # the schema was defined.
        if !declared(sv)
            throw(UnknownSchemaVersionError(sv, provider_name, provider_version))
        end
        try
            Legolas.validate(Tables.schema(table), sv)
        catch
            @warn """
                  The `Tables.Schema` of the `Arrow.Table` read via `Legolas.read(io_or_path)` does not appear to
                  comply with the `Legolas.SchemaVersion` indicated by the table's metadata (`$sv`). Try invoking
                  `Legolas.read(io_or_path; validate=false)` to inspect the table.
                  """
            rethrow()
        end
    end
    return table
end

"""
    Legolas.write(io_or_path, table, sv::SchemaVersion; validate::Bool=true, kwargs...)

Write `table` to `io_or_path`, inserting the appropriate `$LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY`
field in the written out Arrow metadata.

If `validate` is `true`, `Legolas.validate(Tables.schema(table), vs)` will be invoked before the
table is written out to `io_or_path`.

Any other provided `kwargs` are forwarded to an internal invocation of `Arrow.write`.

Note that `io_or_path` may be any type that supports `Base.write(io_or_path, bytes::Vector{UInt8})`.
"""
function write(io_or_path, table, sv::SchemaVersion; validate::Bool=true,
               metadata=Arrow.getmetadata(table), kwargs...)
    if validate
        table_schema = Tables.schema(table)
        if table_schema isa Tables.Schema
            try
                Legolas.validate(table_schema, sv)
            catch
                @warn """
                      The table provided to `Legolas.write` does not appear to comply with the provided `Legolas.SchemaVersion`
                      according to `Legolas.validate`. You may attempt to construct a schema-compliant table by executing
                      `[R(r) for r in Tables.rows(table)]` where `R` is the relevant schema version's record type, or disable
                      validation-on-write by passing `validate=false` to `Legolas.write`.
                      """
                rethrow()
            end
        else
            @warn "could not determine `Tables.Schema` from table provided to `Legolas.write`; skipping schema validation"
        end
    end
    metadata = Set{Pair{String,String}}(isnothing(metadata) ? [] : metadata)
    push!(metadata, LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => identifier(sv))
    provider = schema_provider(sv)
    if !isnothing(provider.name)
        push!(metadata, LEGOLAS_SCHEMA_PROVIDER_NAME_METADATA_KEY => string(provider.name))
    end
    if !isnothing(provider.version)
        push!(metadata, LEGOLAS_SCHEMA_PROVIDER_VERSION_METADATA_KEY => string(provider.version))
    end
    write_arrow(io_or_path, table; metadata=metadata, kwargs...)
    return table
end

"""
    Legolas.tobuffer(args...; kwargs...)

A convenience function that constructs a fresh `io::IOBuffer`, calls
`Legolas.write(io, args...; kwargs...)`, and returns `seekstart(io)`.

Analogous to the `Arrow.tobuffer` function.
"""
function tobuffer(args...; kwargs...)
    io = IOBuffer()
    Legolas.write(io, args...; kwargs...)
    seekstart(io)
    return io
end
