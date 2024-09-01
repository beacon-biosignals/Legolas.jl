module TestProviderPkg

using Legolas: @schema, @version

@schema "test-provider-pkg.foo" Foo

@version FooV1 begin
    a::Int
end

end # module TestProviderPkg
