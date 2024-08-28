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

    # This is largely copy-paste from `ConstructionBase.setproperties_object` (v1.5.7):
    # https://github.com/JuliaObjects/ConstructionBase.jl/blob/71fb5a5198f41f3ef29a53c01940cf7cf6b233eb/src/ConstructionBase.jl#L205-L209
    function ConstructionBase.setproperties(r::R, patch::NamedTuple) where {R<:AbstractRecord}
        ConstructionBase.check_patch_fields_exist(r, patch)
        return ConstructionBase.setfields_object(r, patch)
    end
end

function ConstructionBase.constructorof(::Type{R}) where {R<:AbstractRecord}
    nt = NamedTuple{fieldnames(R)}
    T = Base.typename(R).wrapper
    return (args...) -> T(nt(args))
end

end  # module
