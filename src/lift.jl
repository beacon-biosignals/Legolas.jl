"""
    lift(f, x)

Return `f(x)` unless `x isa Union{Nothing,Missing}`, in which case return `missing`.

This is particularly useful when handling values from `Arrow.Table`, whose null values
may present as either `missing` or `nothing` depending on how the table itself was
originally constructed.

See also: [`lift_type`](@ref)
"""
lift(f, x) = f(x)
lift(::Any, ::Union{Nothing,Missing}) = missing

"""
    lift(f)

Returns a curried function, `x -> lift(f,x)`
"""
lift(f) = Base.Fix1(lift, f)


"""
    lift_type(T::Type, x)

Construct `T(x)` unless `x` is of type `T` or `Missing` then `x` itself will be returned.
If `x` is of type `Nothing` then the value `missing` will be returned instead.

This is particularly useful when handling values from `Arrow.Table`, whose null values
may present as either `missing` or `nothing` depending on how the table itself was
originally constructed.

See also: [`lift`](@ref)
"""
lift_type(T::Type, x) = T(x)
lift_type(::Type{T}, x::T) where T = x
lift_type(::Type, ::Union{Nothing,Missing}) = missing

lift_type(T::Type) = Base.Fix1(lift_type, T)
