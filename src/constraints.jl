struct CheckConstraintError <: Exception
    predicate::Expr
end

function Base.showerror(io::IO, ex::CheckConstraintError)
    print(io, "$CheckConstraintError: $(ex.predicate)")
    return nothing
end

macro check(expr)
    quoted_expr = QuoteNode(expr)
    return esc(:($expr || throw(Legolas.CheckConstraintError($quoted_expr))))
end
