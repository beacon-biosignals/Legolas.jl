"""
    recordmerge(record; fields_to_merge...)

Return a new `AbstractRecord` with the same schema as `record` by merging the fields of
`record` and `fields_to_merge` together using `Tables.rowmerge`.

See also: `Tables.rowmerge`.
"""
function recordmerge(record::AbstractRecord; fields_to_merge...)
    # Avoid using `typeof(record)` as can cause constructor failures with parameterized 
    # record types.
    R = Legolas.record_type(Legolas.schema_version_from_record(record))
    return R(Tables.rowmerge(record; fields_to_merge...))
end
