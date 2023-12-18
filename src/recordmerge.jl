"""
    recordmerge(record; fields_to_merge...)

Return an `AbstractRecord` by merging the fields of `record` and `fields_to_merge` together.

See also: [`Tables.rowmerge`](@ref).
"""
function recordmerge(record::AbstractRecord; fields_to_merge...)
    # Avoid using `typeof(record)` as can cause constructor failures with parameterized 
    # record types.
    R = Legolas.record_type(Legolas.schema_version_from_record(record))
    return R(Tables.rowmerge(record; fields_to_merge...))
end
