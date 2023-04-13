using Legolas
using Legolas: SchemaVersion, @schema, @version
using Tables

@schema "foo" Foo
@version FooV1 begin
    a::Vector
    b::AbstractString
    c::Int32
    d::Int32
end
f = FooV1SchemaVersion()
t = Tables.Schema((:a, :b), (Int32, Float64))
Legolas.validate(t, f)
