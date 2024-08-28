module LegolasConstructionBaseExt

using ConstructionBase
using Legolas

using Legolas: AbstractRecord

# We need a bit of extra work to integrate with ConstructionBase for pre-1.7 due
# to the overload of `propertynames(::Tables.AbstractRow)`. We could overload
# `check_properties_are_fields` but that's not part of the public API, so this
# is safer.
if VERSION < v"1.7"
    ConstructionBase.getproperties(r::AbstractRecord) = NamedTuple(r)

    function ConstructionBase.setproperties(r::R, patch::NamedTuple) where {R<:AbstractRecord}
        if isdefined(ConstructionBase, :check_patch_properties_exist)
            # This is largely copy-paste from `ConstructionBase.setproperties_object`:
            # https://github.com/JuliaObjects/ConstructionBase.jl/blob/cd24e541fd90ab54d2ee12ddd6ccd229be9a5f1e/src/ConstructionBase.jl#L211-L218
            nt = getproperties(r)
            nt_new = merge(nt, patch)
            ConstructionBase.check_patch_properties_exist(nt_new, nt, r, patch)
            args = Tuple(nt_new)  # old Julia inference prefers if we wrap in `Tuple`
            return constructorof(R)(args...)
        else
            # As of ConstructionBase 1.5.7 the internals of `ConstructionBase.setproperties_object` have changed:
            # https://github.com/JuliaObjects/ConstructionBase.jl/blob/71fb5a5198f41f3ef29a53c01940cf7cf6b233eb/src/ConstructionBase.jl#L205-L209
            ConstructionBase.check_patch_fields_exist(r, patch)
            return ConstructionBase.setfields_object(r, patch)
        end
    end
end

function ConstructionBase.constructorof(::Type{R}) where {R<:AbstractRecord}
    nt = NamedTuple{fieldnames(R)}
    T = Base.typename(R).wrapper
    return (args...) -> T(nt(args))
end

end  # module
