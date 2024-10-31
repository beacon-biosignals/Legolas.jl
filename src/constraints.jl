struct CheckConstraintError <: Exception
    predicate::Expr
end

function Base.showerror(io::IO, ex::CheckConstraintError)
    print(io, "$CheckConstraintError: $(ex.predicate)")
    return nothing
end

"""
    @check expr

Define a constraint for a schema version (e.g. `@check x > 0`) from a boolean expression.
The `expr` should evaulate to `true` if the constraint is met or `false` if the constraint
is violated. Multiple constraints may be defined for a schema version. All `@check`
constraints defined with a [`@version`](@ref) must follow all fields defined by the schema
version.

For more details and examples, please see `Legolas.jl/examples/tour.jl`.
"""
macro check(expr)
    quoted_expr = QuoteNode(expr)
    return :($(esc(expr)) || throw(CheckConstraintError($quoted_expr)))
end
