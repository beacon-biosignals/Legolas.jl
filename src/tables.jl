function _columns(table)
    try
        return Tables.columns(table)
    catch
        @warn "encountered error during internal invocation of `Tables.columns`; provided table might not be Tables.jl-compliant"
        rethrow()
    end
end

#####
##### validate tables
#####

"""
    Legolas.extract_schema(table)

Attempt to extract Arrow metadata from `table` via `Arrow.getmetadata(table)`.

If Arrow metadata is present and contains `\"$LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY\" => s`, return [`Legolas.Schema(s)`](@ref).

Otherwise, return `nothing`.
"""
function extract_schema(table)
    metadata = Arrow.getmetadata(table)
    if !isnothing(metadata)
        for (k, v) in metadata
            k == LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY && return Schema(v)
        end
    end
    return nothing
end

"""
    Legolas.validate(table, legolas_schema::Legolas.Schema)

Attempt to determine `s::Tables.Schema` from `table` and return `Legolas.validate(s, legolas_schema)`.

If a `Tables.Schema` cannot be determined, a warning message is logged and `nothing` is returned.
"""
function validate(table, legolas_schema::Schema)
    columns = _columns(table)
    Tables.rowcount(columns) > 0 || return nothing
    tables_schema = Tables.schema(columns)
    if tables_schema isa Tables.Schema
        try
            validate(tables_schema, legolas_schema)
        catch
            @warn "provided table's `Tables.Schema` does not appear to match provided `Legolas.Schema`. Run `[Legolas.Row($legolas_schema, r) for r in Tables.rows(t)]` to try converting the table `t` to a compatible representation."
            rethrow()
        end
    else
        @warn "could not determine `Tables.Schema` from provided table; skipping schema validation"
    end
    return nothing
end

"""
    Legolas.validate(table)

If [`Legolas.extract_schema(table)`](@ref) returns a valid `Legolas.Schema`, return `Legolas.validate(table, Legolas.extract_schema(table))`.

Otherwise, if a `Legolas.Schema` isn't found or is invalid, an `ArgumentError` is thrown.
"""
function validate(table)
    schema = Legolas.extract_schema(table)
    isnothing(schema) && throw(ArgumentError("""
                                             could not extract valid `Legolas.Schema` from provided Arrow table;
                                             is it missing the expected custom metadata and/or the expected
                                             \"$LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY\" field?
                                             """))
    return validate(table, schema)
end

#####
##### read/write tables
#####

"""
    Legolas.read(io_or_path; validate::Bool=true)

Read and return an `Arrow.Table` from `io_or_path`.

If `validate` is `true`, `Legolas.validate` will be called on the table before it is returned.

Note that `io_or_path` may be any type that supports `Base.read(io_or_path)::Vector{UInt8}`.
"""
function read(io_or_path; validate::Bool=true)
    table = read_arrow(io_or_path)
    validate && Legolas.validate(table)
    return table
end

"""
    Legolas.write(io_or_path, table, schema::Schema; validate::Bool=true, kwargs...)

Write `table` to `io_or_path`, inserting the appropriate `$LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY`
field in the written out Arrow metadata.

If `validate` is `true`, `Legolas.validate` will be called on the table before it written out.

Any other provided `kwargs` are forwarded to an internal invocation of `Arrow.write`.

Note that `io_or_path` may be any type that supports `Base.write(io_or_path, bytes::Vector{UInt8})`.
"""
function write(io_or_path, table, schema::Schema; validate::Bool=true,
               metadata=Arrow.getmetadata(table), kwargs...)
    validate && Legolas.validate(table, schema)
    schema_metadata = LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => schema_qualified_string(schema)
    if isnothing(metadata)
        metadata = (schema_metadata,)
    else
        metadata = Set(metadata)
        push!(metadata, schema_metadata)
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
##### Tables.jl operations
#####

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
    gather(column_name, tables...; extract=((table, idxs) -> view(table, idxs, :)))

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
    materialize(table)

Return a fully deserialized copy of `table`.

This function is useful when `table` has built-in deserialize-on-access or
conversion-on-access behavior (like `Arrow.Table`) and you'd like to pay
such access costs upfront before repeatedly accessing the table.

Note that we intend to eventually migrate this function from Legolas.jl to a more appropriate package.
"""
materialize(table) = map(collect, Tables.columntable(table))
