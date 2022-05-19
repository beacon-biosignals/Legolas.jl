"""
    lift(f, x)

Return `f(x)` unless `x isa Union{Nothing,Missing}`, in which case return `missing`.

This is particularly useful when handling values from `Arrow.Table`, whose null values
may present as either `missing` or `nothing` depending on how the table itself was
originally constructed.

See also: [`construct`](@ref)
"""
lift(f, x) = f(x)
lift(::Any, ::Union{Nothing,Missing}) = missing

"""
    lift(f)

Returns a curried function, `x -> lift(f,x)`
"""
lift(f) = Base.Fix1(lift, f)


"""
    construct(T::Type, x)

Construct `T(x)` unless `x` is of type `T`, in which case return `x` itself. Useful in
conjunction with the [`lift`](@ref) function for types which don't have a constructor which
accepts instances of itself (e.g. `T(::T)`).

## Examples

```jldoctest
julia> using Legolas: construct

julia> construct(Float64, 1)
1.0

julia> Some(Some(1))
Some(Some(1))

julia> construct(Some, Some(1))
Some(1)
```

Use the curried form when using `lift`:

```jldoctest
julia> using Legolas: lift, construct

julia> lift(Some, Some(1))
Some(Some(1))

julia> lift(construct(Some), Some(1))
Some(1)
```
"""
construct(T::Type, x) = T(x)
construct(::Type{T}, x::T) where {T} = x
construct(T::Type) = Base.Fix1(construct, T)
