using Compat: current_exceptions
using Legolas, Test, DataFrames, Arrow, UUIDs
using Legolas: @schema, @version, CheckConstraintError, SchemaVersion,
               SchemaVersionDeclarationError, DeclaredFieldInfo
using Accessors
using Aqua
using Pkg

# This test set goes before we load `TestProviderPkg`
@testset "#46: Informative errors when reading unknown schemas from packages" begin
    err = Legolas.UnknownSchemaVersionError(Legolas.SchemaVersion("test-provider-pkg.foo", 1), :TestProviderPkg, v"0.1.0")
    @test_throws err Legolas.read("test_provider_pkg.arrow")
    @test contains(sprint(Base.showerror, err), "TestProviderPkg")

    # Test we can load the table with Arrow.Table as the error message suggests
    table = Arrow.Table("test_provider_pkg.arrow")
    @test table.a[1] == 1
    @test length(table.a) == 1

    # Let's test some more error printing while we're here; if we did not have the VersionNumber
    # (e.g. since the table was generated on Julia pre-1.9), we should still print a reasonable message:
    err = Legolas.UnknownSchemaVersionError(Legolas.SchemaVersion("test-provider-pkg.foo", 1), :TestProviderPkg, missing)
    @test contains(sprint(Base.showerror, err), "TestProviderPkg")
end

# Now load the package, and verify we can write the tables with this metadata
Pkg.develop(; path=joinpath(@__DIR__, "TestProviderPkg"))
using TestProviderPkg

@testset "#46: Writing informative metadata about packages providing schemas" begin
    table = [TestProviderPkg.FooV1(; a=1)]
    Legolas.write("test_provider_pkg.arrow", table, TestProviderPkg.FooV1SchemaVersion())
    table = Legolas.read("test_provider_pkg.arrow")
    v = Legolas.extract_metadata(table, Legolas.LEGOLAS_SCHEMA_PROVIDER_NAME_METADATA_KEY)
    @test v == "TestProviderPkg"

    v = Legolas.extract_metadata(table, Legolas.LEGOLAS_SCHEMA_PROVIDER_VERSION_METADATA_KEY)
    # We currently only write the version on 1.9+ where we can use `pkgversion`
    @test v == (VERSION >= v"1.9-" ? "0.1.0" : nothing)
end

@test_throws SchemaVersionDeclarationError("no prior `@schema` declaration found in current module") @version(TestV1, begin x end)

# Isolate schema defined in the tour from the rest of the tests
module Tour
    include(joinpath(dirname(@__DIR__), "examples", "tour.jl"))
end

@testset "aqua" begin
    # picks up ambiguities in dependencies, and one hit for unbound type
    # parameters for `accepted_field_type(::SchemaVersion, ::Type{Union{Missing,T}})`
    Aqua.test_all(Legolas; ambiguities=false, unbound_args=false)
end

@testset "Legolas.lift" begin
    @test ismissing(Legolas.lift(sin, nothing))
    @test ismissing(Legolas.lift(sin, missing))
    @test Legolas.lift(sin, 1.0) == sin(1.0)
    @test Legolas.lift(Some, Some(1)) == Some(Some(1))

    @test ismissing(Legolas.lift(sin)(nothing))
    @test ismissing(Legolas.lift(sin)(missing))
    @test Legolas.lift(sin)(1.0) == sin(1.0)
    @test Legolas.lift(Some)(Some(1)) == Some(Some(1))
end

@testset "Legolas.construct" begin
    @test Legolas.construct(Int, 0x00) === 0
    @test Legolas.construct(Some, Some(1)) === Some(1)

    @test Legolas.construct(Int)(0x00) === 0
    @test Legolas.construct(Some)(Some(1)) === Some(1)

    # Restrict `construct` to types only
    @test_throws MethodError Legolas.construct(sin, 1.0)
    @test_throws MethodError Legolas.construct(sin)

    @testset "undefined identity constructor" begin
        mutable struct PR45
            x::Int
        end
        Foo = PR45  # Alias to unique struct name

        x = Foo(1)
        @test x !== Foo(1)
        @test_throws MethodError Foo(x)  # Type does not define an identity constructor
        @test Legolas.construct(Foo, x) === x
    end
end

@testset "Legolas.location" begin
    collections = (['a', 'b', 'c', 'f', 'b'],
                   ['d', 'c', 'e', 'b'],
                   ['f', 'a', 'f'])
    expected = Dict('f' => ([4], [], [1, 3]),
                    'a' => ([1], [], [2]),
                    'c' => ([3], [2], []),
                    'd' => ([], [1], []),
                    'e' => ([], [3], []),
                    'b' => ([2, 5], [4], []))
    @test Legolas.locations(collections) == expected
end

@testset "Legolas.gather" begin
    a = [(x=1, y="a", z="k"),
         (x=2, y="b", z="j"),
         (x=4, y="c", z="i"),
         (x=4, y="d", z="h"),
         (x=2, y="e", z="g"),
         (x=5, y="f", z="f"),
         (x=4, y="g", z="e"),
         (x=3, y="h", z="d"),
         (x=1, y="i", z="c"),
         (x=5, y="j", z="b"),
         (x=4, y="k", z="a")]
    b = [(x=1, m=1),
         (x=2, m=2),
         (x=2, m=5),
         (x=5, m=4),
         (x=4, m=6)]
    c = [(test="a", x=1, z=1.0),
         (test="b", x=2, z=1.0),
         (test="d", x=4, z=1.0),
         (test="e", x="gotcha", z=1.0),
         (test="f", x=5, z=1.0),
         (test="h", x=3, z=1.0),
         (test="i", x=1, z=1.0),
         (test="j", x=5, z=1.0),
         (test="k", x=4, z=1.0)]
    dfa, dfb, dfc = DataFrame(a), DataFrame(b), DataFrame(c)
    g = Legolas.gather(:x, a, b, c; extract=(t, i) -> t[i])
    dfg = Legolas.gather(:x, dfa, dfb, dfc)
    expected = Dict(1 => ([(x=1, y="a", z="k"), (x=1, y="i", z="c")],
                          [(x=1, m=1)],
                          [(test="a", x=1, z=1.0), (test="i", x=1, z=1.0)]),
                    2 => ([(x=2, y="b", z="j"), (x=2, y="e", z="g")],
                          [(x=2, m=2), (x=2, m=5)],
                          [(test="b", x=2, z=1.0)]),
                    3 => ([(x=3, y="h", z="d")],
                          NamedTuple{(:x, :m),Tuple{Int64,Int64}}[],
                          [(test="h", x=3, z=1.0)]),
                    4 => ([(x=4, y="c", z="i"), (x=4, y="d", z="h"), (x=4, y="g", z="e"), (x=4, y="k", z="a")],
                          [(x=4, m=6)],
                          [(test="d", x=4, z=1.0), (test="k", x=4, z=1.0)]),
                    5 => ([(x=5, y="f", z="f"), (x=5, y="j", z="b")],
                          [(x=5, m=4)],
                          [(test="f", x=5, z=1.0), (test="j", x=5, z=1.0)]),
                    "gotcha" => (NamedTuple{(:x, :y, :z),NTuple{3,Any}}[],
                                 NamedTuple{(:x, :m),NTuple{2,Any}}[],
                                 [(test="e", x="gotcha", z=1.0)]))
    @test g == expected
    @test keys(dfg) == keys(expected)
    @test all(all(dfg[k] .== DataFrame.(expected[k])) for k in keys(dfg))

    # test both the fast path + fallback path for Legolas._iterator_for_column
    @test Legolas._iterator_for_column(dfa, :x) === dfa.x
    @test Legolas._iterator_for_column(a, :x) == dfa.x
end

bad_id_message(x) = "failed to parse seemingly invalid/malformed schema version identifier string: \"$x\""
bad_name_message(x) = "argument is not a valid `Legolas.SchemaVersion` name: \"$x\""
bad_version_message(x) = "`version` in `SchemaVersion{_,version}` must be a non-negative integer, received: `($x)::$(typeof(x))`"

@testset "Legolas.parse_identifier and related code" begin
    good_schema_names = ("foo", "test.foo", "test.foo-bar", ".-technically-allowed-.")
    good_versions = (0, 1, 2, 3)
    bad_schema_names = ("has_underscore", "caPitaLs",  "has a space", "illegal?chars*")
    bad_versions = (-1, -2, -3)

    for n in good_schema_names, v in good_versions
        @test Legolas.parse_identifier("$n@$v") == [SchemaVersion(n, v)]
        @test SchemaVersion(n, v) == SchemaVersion{Symbol(n),v}()
        @test Legolas.name(SchemaVersion(n, v)) == Symbol(n)
        @test Legolas.version(SchemaVersion(n, v)) == v
    end

    for n in good_schema_names, v in bad_versions
        @test_throws ArgumentError(bad_version_message(v)) SchemaVersion(n, v)
        id = "$n@$v"
        @test_throws ArgumentError(bad_version_message(v)) Legolas.parse_identifier(id)
    end

    for n in bad_schema_names, v in Iterators.flatten((bad_versions, good_versions))
        @test_throws ArgumentError(bad_name_message(n)) SchemaVersion(n, v)
        id = "$n@$v"
        @test_throws ArgumentError(bad_name_message(n)) Legolas.parse_identifier(id)
    end

    for n in good_schema_names, m in good_schema_names
        @test Legolas.parse_identifier("$n@3>$m@2") == [SchemaVersion(n, 3), SchemaVersion(m, 2)]
        @test Legolas.parse_identifier("$n@1>$m@0>bob@3") == [SchemaVersion(n, 1), SchemaVersion(m, 0), SchemaVersion("bob", 3)]
        @test Legolas.parse_identifier("$n@1 >$m@0 > bob@3 ") == [SchemaVersion(n, 1), SchemaVersion(m, 0), SchemaVersion("bob", 3)]
        id = "$n@1 >$m @0 > bob@3 "
        @test_throws ArgumentError(bad_name_message("$m ")) Legolas.parse_identifier(id)
        for bad_id in ("$n>$m@1",
                       "$n@1>$m",
                       "$n@>$m@",
                       "$n>$m@",
                       "$n@>$m",
                       "$n>$m")
            @test_throws ArgumentError(bad_id_message(bad_id)) Legolas.parse_identifier(bad_id)
        end
        for bad_separator in ("<", "<:", ":", "=")
            id = "$n@1" * bad_separator * "$m@0"
            @test_throws ArgumentError(bad_id_message(id)) Legolas.parse_identifier(id)
        end
    end

    for good in good_schema_names, bad in bad_schema_names
        for id in ("$good@3>$bad@2",
                   "$bad@1>bob@0>$good@3",
                   "bob@1>$bad@0>$good@3",
                   "$good@1>bob@0>$bad@3")
            @test_throws ArgumentError(bad_name_message(bad)) Legolas.parse_identifier(id)
        end
    end
end

@testset "Legolas.@schema" begin
    @test_throws ArgumentError("`name` provided to `@schema` must be a string literal") @schema(joe, J)
    @test_throws ArgumentError("`name` provided to `@schema` should not include an `@` version clause") @schema("joe@1", J)
    @test_throws ArgumentError("`name` provided to `@schema` is not a valid `Legolas.SchemaVersion` name: \"joe?\"") @schema("joe?", J)
    @test_throws ArgumentError("`Prefix` provided to `@schema` is not a valid type name: J{Int}") @schema("joo", J{Int})
    @test isnothing(@schema "test.returns-nothing" ReturnsNothing)
end

# https://github.com/beacon-biosignals/Legolas.jl/issues/91
module Namespace91
    using Test
    using Legolas: @schema, @version, tobuffer, read
    using Accessors: @set

    # Define something else with the name Legolas
    struct Legolas end

    @schema "test.a91" A91

    @version A91V1 begin
        a::Int
    end

    @testset "Macro invocation doesn't require Legolas module name in caller's scope" begin
        # Try out a bunch of the auto-generated methods
        # to ensure the wrong `Legolas` name isn't hidden in one of them
        @test A91V1(; a=1) isa A91V1
        @test hash(A91V1(; a=1)) isa UInt
        @test A91V1(; a=1) == A91V1(; a=1)
        @test isequal(A91V1(; a=1), A91V1(; a=1))
        @test read(tobuffer([A91V1(; a=1)], A91V1SchemaVersion())).a == [1]

        a = A91V1(; a=1)
        a2 = @set a.a = 2
        @test a2.a == 2
    end
end # module

@schema "test.parent" Parent
@version ParentV1 begin
    x::Vector
    y::AbstractString
end

@schema "test.child" Child
@version ChildV1 > ParentV1 begin
    z
end

@schema "test.grandchild" Grandchild
@version GrandchildV1 > ChildV1 begin
    a::Int32 = round(Int32, a)
    y::String = string(y[1:2])
end

@schema "test.great-grandchild" GreatGrandchild
@version GreatGrandchildV1 > GrandchildV1

@schema "test.nested" Nested
@version NestedV1 begin
    gc::GrandchildV1
    k::(<:Any)
end

@schema "test.nested-again" NestedAgain
@version NestedAgainV1 begin
    n::(<:NestedV1)
    h::(<:Any)
end

# This statement will induce an error if field types are not properly escaped,
# since `DataFrame` will be hygeine-passed to `Legolas.DataFrame`, which is undefined
@schema "test.field-type-escape" FieldTypeEscape
@version FieldTypeEscapeV1 begin
    x::DataFrame
end

@schema "test.accepted" Accepted
@version AcceptedV1 begin
    id::UUID
    sym::Symbol
end

@schema "test.new" New

@schema "test.has-a-v1-in-the-middle" HasAV1InTheMiddle
@version HasAV1InTheMiddleV1 begin
    id::UUID
end
@version HasAV1InTheMiddleV2 begin
    id::UUID
    x::Int
end

@schema "test.documented" Documented

"""
    DocumentedV1

Very detailed documentation.
"""
@version DocumentedV1 begin
    x
end

@schema "test.param" Param

@version ParamV1 begin
    i::(<:Integer)
end

@schema "test.union-with-param" UnionWithParam

@version UnionWithParamV1 begin
    xs::Union{Vector{String},Missing}
end

@schema "test.union-missing" UnionMissing

@version UnionMissingV1 begin
    a::Union{Int,Missing}
    b::Union{Int,Missing}
end

@schema "test.empty-inside" EmptyInside

@version EmptyInsideV1

@schema "test.missing" MissingOnly

@version MissingOnlyV1 begin
    i::Missing
end

@schema "test.issue-94-child" Issue94Child

@version Issue94ChildV1 begin
    b::Int
    a::Int
end

@schema "test.issue-94-parent" Issue94Parent

@version Issue94ParentV1 begin
    x::Issue94ChildV1
end


@testset "`Legolas.@version` and associated utilities for declared `Legolas.SchemaVersion`s" begin
    @testset "Legolas.SchemaVersionDeclarationError" begin
        @test_throws SchemaVersionDeclarationError("missing prior `@schema` declaration for `Unknown` in current module") @version(UnknownV1 > ChildV1, begin x end)
        @test_throws SchemaVersionDeclarationError("provided record type symbol is malformed: Child") @version(Child, begin x end)
        @test_throws SchemaVersionDeclarationError("provided record type symbol is malformed: Childv2") @version(Childv2, begin x end)
        @test_throws SchemaVersionDeclarationError("provided record type symbol is malformed: ChildV") @version(ChildV, begin x end)
        @test_throws SchemaVersionDeclarationError("provided record type symbol is malformed: ChildVTwo") @version(ChildVTwo, begin x end)
        @test_throws SchemaVersionDeclarationError("provided record type symbol is malformed: Child") @version(Child > ParentV2, begin x end)
        @test_throws SchemaVersionDeclarationError("provided record type symbol is malformed: Childv2") @version(Childv2 > ParentV2, begin x end)
        @test_throws SchemaVersionDeclarationError("provided record type symbol is malformed: ChildV") @version(ChildV > ParentV2, begin x end)
        @test_throws SchemaVersionDeclarationError("provided record type symbol is malformed: ChildVTwo") @version(ChildVTwo > ParentV2, begin x end)
        @test_throws SchemaVersionDeclarationError("provided record type expression is malformed: BobV1 > DaveV1 > JoeV1") @version(BobV1 > DaveV1 > JoeV1, begin x end)
        @test_throws SchemaVersionDeclarationError("provided record type expression is malformed: BobV1 < DaveV1") @version(BobV1 < DaveV1, begin x end)
        @test_throws SchemaVersionDeclarationError("cannot have duplicate field names in `@version` declaration; received: $([:x, :y, :x, :z])") @version(ChildV2, begin x; y; x; z end)
        @test_throws SchemaVersionDeclarationError("cannot have field name which start with an underscore in `@version` declaration: $([:_X])") @version(ChildV2, begin x; X; _X end)
        @test_throws SchemaVersionDeclarationError("cannot extend from another version of the same schema") @version(ChildV2 > ChildV1, begin x end)
        @test_throws SchemaVersionDeclarationError("declared field types violate parent's field types") @version(NewV1 > ParentV1, begin y::Int end)
        @test_throws SchemaVersionDeclarationError("declared field types violate parent's field types") @version(NewV1 > ChildV1, begin y::Int end)
        @test_throws SchemaVersionDeclarationError("invalid redeclaration of existing schema version; all `@version` redeclarations must exactly match previous declarations") @version(ParentV1, begin x; y end)
        @test_throws SchemaVersionDeclarationError("malformed `@version` field expression: f()") @version(ChildV2, begin f() end)
    end

    @test_throws UndefVarError(:UnknownV1) @version(ChildV1 > UnknownV1, begin x end)

    undeclared = SchemaVersion("undeclared", 3)

    @testset "Legolas.declared" begin
        @test !Legolas.declared(undeclared)
        @test all(Legolas.declared, (ParentV1SchemaVersion(), ChildV1SchemaVersion(), GrandchildV1SchemaVersion(), GreatGrandchildV1SchemaVersion()))
    end

    @testset "Legolas.parent" begin
        @test isnothing(Legolas.parent(undeclared))
        @test isnothing(Legolas.parent(ParentV1SchemaVersion()))
        @test Legolas.parent(ChildV1SchemaVersion()) == ParentV1SchemaVersion()
        @test Legolas.parent(GrandchildV1SchemaVersion()) == ChildV1SchemaVersion()
        @test Legolas.parent(GreatGrandchildV1SchemaVersion()) == GrandchildV1SchemaVersion()
    end

    @testset "Legolas.identifier" begin
        @test_throws Legolas.UnknownSchemaVersionError(undeclared) Legolas.identifier(undeclared)
        @test Legolas.identifier(ParentV1SchemaVersion()) == "test.parent@1"
        @test Legolas.identifier(ChildV1SchemaVersion()) == "test.child@1>test.parent@1"
        @test Legolas.identifier(GrandchildV1SchemaVersion()) == "test.grandchild@1>test.child@1>test.parent@1"
        @test Legolas.identifier(GreatGrandchildV1SchemaVersion()) == "test.great-grandchild@1>test.grandchild@1>test.child@1>test.parent@1"
    end

    @testset "Legolas.declared_fields" begin
        @test_throws Legolas.UnknownSchemaVersionError(undeclared) Legolas.declared_fields(undeclared)
        @test Legolas.declared_fields(EmptyInsideV1SchemaVersion()) == NamedTuple()
        @test Legolas.declared_fields(ParentV1SchemaVersion()) == (x=Vector, y=AbstractString)
        @test Legolas.declared_fields(ChildV1SchemaVersion()) == (x=Vector, y=AbstractString, z=Any)
        @test Legolas.declared_fields(GrandchildV1SchemaVersion()) == (x=Vector, y=String, z=Any, a=Int32)
        @test Legolas.declared_fields(GreatGrandchildV1SchemaVersion()) == (x=Vector, y=String, z=Any, a=Int32)

        # xref https://github.com/beacon-biosignals/Legolas.jl/pull/100
        @test Legolas.declared_fields(ParentV1SchemaVersion()) == (@test_deprecated Legolas.required_fields(ParentV1SchemaVersion()))
        @test Legolas.declared_fields(ChildV1SchemaVersion()) == (@test_deprecated Legolas.required_fields(ChildV1SchemaVersion()))
        @test Legolas.declared_fields(GrandchildV1SchemaVersion()) == (@test_deprecated Legolas.required_fields(GrandchildV1SchemaVersion()))
    end

    @testset "Legolas.find_violation + Legolas.complies_with + Legolas.validate" begin
        @test_throws Legolas.UnknownSchemaVersionError(undeclared) Legolas.validate(Tables.Schema((:a, :b), (Int, Int)), undeclared)
        @test_throws Legolas.UnknownSchemaVersionError(undeclared) Legolas.complies_with(Tables.Schema((:a, :b), (Int, Int)), undeclared)
        @test_throws Legolas.UnknownSchemaVersionError(undeclared) Legolas.find_violation(Tables.Schema((:a, :b), (Int, Int)), undeclared)

        @test Legolas.complies_with(Tables.Schema((), ()), EmptyInsideV1SchemaVersion())

        # Note that many of the basic properties of `find_violation`/`complies_with`/`validate`
        # are unit-tested in `examples/tour.jl`; thus, we focus here on testing that these
        # functions work as expected w.r.t. schema extension in particular.

        t = Tables.Schema((:a, :y, :z), (Int32, String, Any))
        for (s, id) in ((GrandchildV1SchemaVersion(), "test.grandchild@1"),
                        (ChildV1SchemaVersion(), "test.child@1"),
                        (ParentV1SchemaVersion(), "test.parent@1"))
            msg = """
                  Tables.Schema violates Legolas schema `$id`:
                   - Could not find declared field: `x`
                  Provided Tables.Schema:
                   :a  Int32
                   :y  String
                   :z  Any"""
            @test_throws ArgumentError(msg) Legolas.validate(t, s)
            @test !Legolas.complies_with(t, s)
            @test isequal(Legolas.find_violation(t, s), :x => missing)
            @test isequal(Legolas.find_violations(t, s), [:x => missing])
        end

        # Multiple missing field violations
        t = Tables.Schema((:a, :z), (Int32, Any))
        for (s, id) in ((GreatGrandchildV1SchemaVersion(), "test.great-grandchild@1"),
                        (GrandchildV1SchemaVersion(), "test.grandchild@1"),
                        (ChildV1SchemaVersion(), "test.child@1"),
                        (ParentV1SchemaVersion(), "test.parent@1"))
            msg = """
                  Tables.Schema violates Legolas schema `$id`:
                   - Could not find declared field: `x`
                   - Could not find declared field: `y`
                  Provided Tables.Schema:
                   :a  Int32
                   :z  Any"""
            @test_throws ArgumentError(msg) Legolas.validate(t, s)
            @test !Legolas.complies_with(t, s)
            @test isequal(Legolas.find_violation(t, s), :x => missing)
            @test isequal(Legolas.find_violations(t, s), [:x => missing, :y => missing])
        end

        # Multiple violations, both missing fields and type error
        t = Tables.Schema((:y, :a), (Bool, Int32))
        let s = GrandchildV1SchemaVersion()
            msg = """
            Tables.Schema violates Legolas schema `test.grandchild@1`:
             - Could not find declared field: `x`
             - Incorrect type: `y` expected `<:String`, found `Bool`
            Provided Tables.Schema:
             :y  Bool
             :a  Int32"""
            @test_throws ArgumentError(msg) Legolas.validate(t, s)
            @test !Legolas.complies_with(t, s)
            @test isequal(Legolas.find_violation(t, s), :x => missing)
            @test isequal(Legolas.find_violations(t, s), [:x => missing, :y => Bool])
        end

        t = Tables.Schema((:x, :a, :y), (Vector, Int32, String))
        for s in (GrandchildV1SchemaVersion(), ChildV1SchemaVersion(), ParentV1SchemaVersion())
            @test isnothing(Legolas.validate(t, s))
            @test Legolas.complies_with(t, s)
            @test isnothing(Legolas.find_violation(t, s))
            @test isempty(Legolas.find_violations(t, s))
        end

        for T in (UUID, UInt128), S in (Symbol, String)
            @test Legolas.complies_with(Tables.Schema((:id, :sym), (T, S)), AcceptedV1SchemaVersion())
        end

        t = Tables.Schema((:xs,), (SubArray{String,1,Arrow.List{String,Int32,Vector{UInt8}},
                                            Tuple{UnitRange{Int64}},true},))
        s = UnionWithParamV1SchemaVersion()
        @test isnothing(Legolas.validate(t, s))
        @test Legolas.complies_with(t, s)
        @test isnothing(Legolas.find_violation(t, s))
        @test isempty(Legolas.find_violations(t, s))
    end

    @testset "Legolas.declaration" begin
        @test_throws Legolas.UnknownSchemaVersionError(undeclared) Legolas.declaration(undeclared)
        @test Legolas.declaration(ParentV1SchemaVersion()) == ("test.parent@1" => [DeclaredFieldInfo(:x, :Vector, false, :(x::Vector = x)),
                                                                                   DeclaredFieldInfo(:y, :AbstractString, false, :(y::AbstractString = y))])
        @test Legolas.declaration(ChildV1SchemaVersion()) == ("test.child@1>test.parent@1" => [DeclaredFieldInfo(:z, :Any, false, :(z::Any = z))])
        @test Legolas.declaration(GrandchildV1SchemaVersion()) == ("test.grandchild@1>test.child@1" => [DeclaredFieldInfo(:a, :Int32, false, :(a::Int32 = round(Int32, a))),
                                                                                                        DeclaredFieldInfo(:y, :String, false, :(y::String = string(y[1:2])))])
        @test Legolas.declaration(GreatGrandchildV1SchemaVersion()) == ("test.great-grandchild@1>test.grandchild@1" => DeclaredFieldInfo[])
        @test Legolas.DeclaredFieldInfo === Legolas.RequiredFieldInfo # xref https://github.com/beacon-biosignals/Legolas.jl/pull/100
    end

    @testset "Legolas.record_type" begin
        @test_throws Legolas.UnknownSchemaVersionError(undeclared) Legolas.record_type(undeclared)
        @test Legolas.record_type(ParentV1SchemaVersion()) == ParentV1
        @test Legolas.record_type(ChildV1SchemaVersion()) == ChildV1
        @test Legolas.record_type(GrandchildV1SchemaVersion()) == GrandchildV1
        @test Legolas.record_type(GreatGrandchildV1SchemaVersion()) == GreatGrandchildV1
    end

    r0 = (x=[42], y="foo", z=:three, a=1.3)
    r0_arrow = first(Tables.rows(Arrow.Table(Arrow.tobuffer([r0]))))

    @test NamedTuple(ParentV1(r0)) == (; r0.x, r0.y)
    @test ParentV1(r0) == ParentV1(; r0.x, r0.y)
    @test ParentV1(r0) == ParentV1(r0_arrow)

    @test NamedTuple(ChildV1(r0)) == (; r0.x, r0.y, r0.z)
    @test ChildV1(r0) == ChildV1(; r0.x, r0.y, r0.z)
    @test ChildV1(r0) == ChildV1(r0_arrow)

    @test NamedTuple(GrandchildV1(r0)) == (x=[42], y="fo", z=:three, a=1)
    @test GrandchildV1(r0) == GrandchildV1(; r0.x, r0.y, r0.z, r0.a)
    @test GrandchildV1(r0) == GrandchildV1(r0_arrow)

    @test Legolas.schema_version_from_record(ParentV1(r0)) == ParentV1SchemaVersion()
    @test Legolas.schema_version_from_record(ChildV1(r0)) == ChildV1SchemaVersion()
    @test Legolas.schema_version_from_record(GrandchildV1(r0)) == GrandchildV1SchemaVersion()

    tbl = Arrow.Table(Arrow.tobuffer((; x=[ParentV1(r0)])))
    @test tbl.x[1] == ParentV1(Tables.rowmerge(r0))

    # Note that Arrow.jl roundtrips z=:three to z="three", since
    # `z::Symbol` isn't evident from these record types
    r0_roundtripped = Tables.rowmerge(r0; z="three")

    tbl = Arrow.Table(Arrow.tobuffer((; x=[ChildV1(r0)])))
    @test tbl.x[1] == ChildV1(r0_roundtripped)

    tbl = Arrow.Table(Arrow.tobuffer((; x=[GrandchildV1(r0)])))
    @test tbl.x[1] == GrandchildV1(r0_roundtripped)

    svs = [GrandchildV1SchemaVersion(), ChildV1SchemaVersion(), ParentV1SchemaVersion()]
    tbl = Arrow.Table(Arrow.tobuffer((; sv=svs)))
    @test all(tbl.sv .== svs)

    tbl = [NestedV1(; gc=GrandchildV1(r0), k="test")]
    roundtripped = Legolas.read(Legolas.tobuffer(tbl, NestedV1SchemaVersion()))
    @test roundtripped.gc[1] == GrandchildV1(r0_roundtripped)
    @test roundtripped.k[1] == "test"

    tbl = [NestedAgainV1(; n=NestedV1(; gc=GrandchildV1(r0), k="test"), h=3)]
    roundtripped = Legolas.read(Legolas.tobuffer(tbl, NestedAgainV1SchemaVersion()))
    @test roundtripped.n[1] == NestedV1(; gc=GrandchildV1(r0_roundtripped), k="test")
    @test roundtripped.h[1] == 3

    @testset "deserialization agnostic to serialized field order" begin
        @test Arrow.Table("issue-94.arrow").x[1] == Issue94ChildV1(; a=1, b=2)
    end

    @testset "docstring support" begin
        ds = string(@doc DocumentedV1)
        @test contains(ds, "Very detailed documentation")
    end

    @testset "parameterized" begin
        @test typeof(ParamV1(; i=1)) === ParamV1{Int}
        @test typeof(ParamV1{Integer}(; i=1)) === ParamV1{Integer}
        @test typeof(ParamV1{Int}(; i=1.0)) === ParamV1{Int}
        @test_throws TypeError ParamV1{Float64}(; i=1)
        @test_throws TypeError ParamV1(; i=1.0)
        @test_throws ArgumentError ParamV1{Int}(; i=1.1)
    end

    @testset "equality and hashing" begin
        a = ChildV1(; x=[1, 2], y="hello", z=missing)
        b = ChildV1(; x=[1, 2], y="hello", z=missing)
        @test a !== b
        @test isequal(a, b)
        @test ismissing(a == b)
        @test hash(a) == hash(b)
        u1 = UnionMissingV1(; a=missing, b=1)
        u2 = UnionMissingV1(; a=missing, b=2)
        @test u1 != u2
        @test !isequal(u1, u2)
        p32 = ParamV1(; i=one(Int32))
        p64 = ParamV1(; i=one(Int64))
        @test p32 == p64
        @test isequal(p32, p64)
        🧑 = ParentV1(; x=[4, 20], y="")
        🧒 = ChildV1(; x=[4, 20], y="", z=missing)
        @test 🧑 != 🧒
        @test !isequal(🧑, 🧒)
    end

    @testset "strict Missing validation" begin
        row = MissingOnlyV1()
        @test Legolas.complies_with(Tables.schema([row]), MissingOnlyV1SchemaVersion())
    end
end

@testset "miscellaneous Legolas/src/tables.jl tests" begin
    struct MyPath
        x::String
    end
    Base.read(p::MyPath) = Base.read(p.x)
    Base.write(p::MyPath, bytes) = Base.write(p.x, bytes)
    root = mktempdir()
    path = MyPath(joinpath(root, "baz.arrow"))
    t = [(x=[1,2], y="hello"), (x=[3,4], y="bye")]
    Legolas.write(path, t, ParentV1SchemaVersion())
    @test t == [NamedTuple(ParentV1(r)) for r in Tables.rows(Legolas.read(path))]
    tbl = Arrow.Table(Legolas.tobuffer(t, ParentV1SchemaVersion(); metadata=("a" => "b", "c" => "d")))
    @test Set(Arrow.getmetadata(tbl)) == Set((Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "test.parent@1",
                                              "a" => "b", "c" => "d"))

    struct Moo
        meta
    end
    Legolas.Arrow.getmetadata(moo::Moo) = moo.meta
    moo = Moo(Dict("a" => "b", "b" => "b", Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "test.parent@1"))
    @test ParentV1SchemaVersion() == Legolas.extract_schema_version(moo)

    t = Arrow.tobuffer((a=[1, 2], b=[3, 4]); metadata=Dict(Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "haha@3"))
    @test_throws Legolas.UnknownSchemaVersionError Legolas.read(t)
end

#####
##### cross-module `@schema`/`@version` declaration behaviors (ref https://github.com/beacon-biosignals/Legolas.jl/issues/65)
#####

module A
    using Legolas
    Legolas.@schema "a.cross" Cross
    Legolas.@version CrossV1 begin x::Number end
end

module B
    # this module tests referencing extenal module's record types in the parent position
    using ..A
    using Legolas
    Legolas.@schema "b.cross" Cross
    Legolas.@version CrossV1 > A.CrossV1 begin x::Int end
end

@test A.CrossV1 <: Legolas.AbstractRecord
@test Arrow.ArrowTypes.arrowname(A.CrossV1) == Symbol("JuliaLang.Legolas.Generated.a.cross.1")
@test B.CrossV1 <: Legolas.AbstractRecord
@test Arrow.ArrowTypes.arrowname(B.CrossV1) == Symbol("JuliaLang.Legolas.Generated.b.cross.1")
@test Legolas.parent(B.CrossV1SchemaVersion()) == A.CrossV1SchemaVersion()

# test that one cannot declare new versions of a schema declared in a different module
@test_throws SchemaVersionDeclarationError("provided record type expression is malformed: A.CrossV2") @version(A.CrossV2, begin x::Int end)
@test_throws SchemaVersionDeclarationError("missing prior `@schema` declaration for `Cross` in current module") @version(CrossV2, begin x::Int end)

# overwriting another module's reserved schema name is disallowed
@test_throws ArgumentError("A schema with this name was already declared by a different module: $A") @schema("a.cross", Cross)

#####
##### local field variable handling in record constructors (ref https://github.com/beacon-biosignals/Legolas.jl/issues/76)
#####

Legolas.@schema "unconstrained-field" UnconstrainedField

Legolas.@version UnconstrainedFieldV1 begin
    field::Any
end

Legolas.@schema "constrained-field" ConstrainedField

Legolas.@version ConstrainedFieldV1 > UnconstrainedFieldV1 begin
    field::Int = parse(Int, field)
end

c = ConstrainedFieldV1(field = "1")
@test c.field == 1

#####
##### record error reporting
#####

@schema "test.field-error" FieldError

function _validate(x)
    x in ("a", "b", "c") || throw(ArgumentError("Must be a, b, or c"))
    return x
end

@version FieldErrorV1 begin
    a::Union{String,Missing} = Legolas.lift(_validate, a)
    b::(<:Union{String,Missing}) = Legolas.lift(_validate, b)
    c::Union{Integer,Missing}
    d::(<:Union{Integer,Missing})
end

_num_calls = Ref{Int}(0)
@version FieldErrorV2 begin
    a::Integer = begin
        _num_calls[] += 1
        a isa Function ? a() : a
    end
end

@version FieldErrorV3 begin
    a::Integer = replace(a, ' ' => '-')
end

@testset "Legolas record constructor error handling" begin
    @testset "field constructor error" begin
        ex_stack = try
            FieldErrorV1(; a="invalid")
        catch
            current_exceptions()
        end

        @test length(ex_stack) == 2
        @test sprint(showerror, ex_stack[1].exception) == "ArgumentError: Must be a, b, or c"
        @test sprint(showerror, ex_stack[2].exception) == "ArgumentError: Invalid value set for field `a` (\"invalid\")"

        ex_stack = try
            FieldErrorV1(; b="invalid")
        catch
            current_exceptions()
        end

        @test length(ex_stack) == 2
        @test sprint(showerror, ex_stack[1].exception) == "ArgumentError: Must be a, b, or c"
        @test sprint(showerror, ex_stack[2].exception) == "ArgumentError: Invalid value set for field `b` (\"invalid\")"

        ex_stack = try
            FieldErrorV1(; c="3")
        catch
            current_exceptions()
        end

        @test length(ex_stack) == 2
        @test startswith(sprint(showerror, ex_stack[1].exception), "MethodError: Cannot `convert` an object of type String to an object of type Integer")
        @test sprint(showerror, ex_stack[2].exception) == "ArgumentError: Invalid value set for field `c`, expected Union{Missing, Integer}, got a value of type String (\"3\")"

        ex_stack = try
            FieldErrorV1(; d=4.0)
        catch
            current_exceptions()
        end

        @test length(ex_stack) == 1
        @test sprint(showerror, ex_stack[1].exception) == "TypeError: in FieldErrorV1, in field `d`, expected Union{Missing, Integer}, got a value of type Float64"

        ex_stack = try
            FieldErrorV1{Missing,Int}(; d=4.5)
        catch
            current_exceptions()
        end

        @test length(ex_stack) == 2
        @test typeof(ex_stack[1].exception) == InexactError
        @test sprint(showerror, ex_stack[2].exception) == "ArgumentError: Invalid value set for field `d`, expected Union{Missing, Integer}, got a value of type Float64 (4.5)"

        ex_stack = try
            FieldErrorV1{AbstractString,Int}
        catch
            current_exceptions()
        end

        @test length(ex_stack) == 1
        @test contains(sprint(showerror, ex_stack[1].exception), r"TypeError: in FieldErrorV1, in _B, expected _B<:Union{Missing, String}, got Type{AbstractString}")
    end

    @testset "one-time evaluation" begin
        _num_calls[] = 0
        FieldErrorV2(; a=1.0)
        @test _num_calls[] == 1

        _num_calls[] = 0
        @test_throws ArgumentError FieldErrorV2(; a=() -> error("foo"))
        @test _num_calls[] == 1

        _num_calls[] = 0
        @test_throws ArgumentError FieldErrorV2(; a="a")
        @test _num_calls[] == 1
    end

    @testset "reports modifications" begin
        e = ArgumentError("Invalid value set for field `a`, expected Integer, got a value of type String (\"foo-bar\")")
        @test_throws e FieldErrorV3(; a="foo bar")
    end
end

#####
##### ConstructionBase/Accessors integration
#####

@testset "ConstructionBase/Accessors integration" begin
    p = ParentV1(; x=[1,2,3], y="hello")
    p2 = @set p.y = "okay"
    @test p2 isa ParentV1
    @test p2.y == "okay"
    @test p2 !== p
    @test p2.x === p.x
    @test p2.y != p.y

    e = ArgumentError("Invalid value set for field `y`, expected AbstractString, got a value of type Int64 (1)")
    @test_throws e @set p.y = 1

    n = NestedV1(; gc=GrandchildV1(; x=[1,2], y="hello", z=nothing, a=1), k=:okay)
    n2 = @set n.gc.z = "yay!"
    @test n2.gc.z == "yay!"
    @test n2.gc != n.gc
    # parametric on :k
    @test typeof(n2) == typeof(n)
    @test n2.k === n.k

    n3 = @set n.k = missing
    @test n3 isa NestedV1{Missing}
    @test ismissing(n3.k)
end

@testset "Legolas.record_merge" begin
    r = Legolas.record_merge(ParentV1(; x=1:3, y="a"); y="b")
    @test r isa ParentV1
    @test r.x == 1:3
    @test r.y == "b"

    r = Legolas.record_merge(ChildV1(; x=1:3, y="a", z=1); y="c", z=2)
    @test r isa ChildV1
    @test r.x == 1:3
    @test r.y == "c"
    @test r.z == 2

    r = Legolas.record_merge(ParamV1(; i=UInt8(0)); i=UInt16(1))
    @test r isa ParamV1{UInt16}
    @test r.i isa UInt16
    @test r.i == 1
end

#####
##### constraints
#####

@schema "test.constraint" Constraint

const CONSTRAINT_V1_EQUAL_CONSTRAINT_LINE = @__LINE__() + 4
@version ConstraintV1 begin
    a
    b = clamp(b, 0, 5)
    @check a == b
    @check a > 0
end

@testset "constraints" begin
    r = ConstraintV1(; a=1, b=1)
    @test r isa ConstraintV1
    @test r.a === 1
    @test r.b === 1

    r = ConstraintV1(; a=1, b=1.0)
    @test r isa ConstraintV1
    @test r.a === 1
    @test r.b === 1.0

    # In Julia 1.8+ we can test can test against "CheckConstraintError: a == b"
    try
        ConstraintV1(; a=1, b=2)
        @test false  # Fail safe if the above line doesn't throw
    catch e
        @test e isa CheckConstraintError
        @test e.predicate == :(a == b)
    end

    try
        ConstraintV1(; a=0, b=0)
        @test false  # Fail safe if the above line doesn't throw
    catch e
        @test e isa CheckConstraintError
        @test e.predicate == :(a > 0)
    end

    try
        ConstraintV1(; a=6, b=6)
        @test false  # Fail safe if the above line doesn't throw
    catch e
        @test e isa CheckConstraintError
        @test e.predicate == :(a == b)
    end

    # For exceptions that occur during processing constraints its convenient to include the
    # location of the `@check` in the stacktrace.
    try
        ConstraintV1(; a=1, b=missing)  # Fails when running check `a == b`
        @test false  # Fail safe if the above line doesn't throw
    catch e
        @test e isa TypeError

        bt = Base.process_backtrace(catch_backtrace())
        sf = bt[1][1]::Base.StackFrame
        @test string(sf.file) == @__FILE__
        @test sf.line == CONSTRAINT_V1_EQUAL_CONSTRAINT_LINE
    end
end

@testset "constraints must be after all fields" begin
    @test_throws SchemaVersionDeclarationError @version(ConstraintV2, begin a; @check a == 1; b end)
end

@testset "CheckConstraintError" begin
    @test sprint(showerror, CheckConstraintError(:(1 == 2))) == "CheckConstraintError: 1 == 2"
end
