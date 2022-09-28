using Legolas, Test, DataFrames, Arrow

using Legolas: Schema, @schema, apply

include(joinpath(dirname(@__DIR__), "examples", "tour.jl"))

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

expected_bad_name_message(x) = "argument is not a valid `Legolas.Schema` name: \"$x\""
expected_bad_id_message(x) = "argument is not a valid `Legolas.Schema` identifier: \"$x\""
expected_bad_version_message(x) = "`Legolas.Schema` version must be non-negative, received: $x"

function generate_test_schema_identifiers(good_names, bad_names)
    # [(schema identifier, expected schemas), ...]
    good_identifiers = Pair{String,Vector{Schema}}[]
    # [(schema identifier, expected error message), ...]

    bad_identifiers = Tuple{Pair{String,String},Pair{Expr,String}}[]
    for good in good_names
        push!(good_identifiers, "$good@1" => "$good@1")
        push!(bad_identifiers, (good => expected_bad_id_message(good), Expr(:quote, good) => "TODO"))
        for other_good in good_names
            good == other_good && continue
            push!(good_identifiers, "$good@1" => "$good@1>$other_good@1")
            for bad in ("$good>$other_good@1",
                        "$good@1>$other_good",
                        ("$good@1 $s $other_good@1" for s in ("<", "<:", ":", "="))...)
                push!(bad_identifiers, (bad => expected_bad_id_message(bad), Expr(:quote, bad) => "TODO"))
            end
        end
    end
    # for bad in bad_names
    #     push!(bad_identifiers, "$bad" => Expr(:quote, bad))
    #     bad = "$bad@1"
    #     push!(bad_identifiers, "$bad" => Expr(:quote, bad))
    #     for good in good_names
    #         good = "$good@1"
    #         push!(bad_identifiers, "$bad>$good" => Expr(:(>), bad, good))
    #         push!(bad_identifiers, "$good>$bad" => Expr(:(>), good, bad))
    #     end
    # end
    return good_identifiers, bad_identifiers
end

@testset "`Legolas.Schema`" begin
    good_schema_names = ("foo", "test.foo", "test.foo-bar", ".-technically-allowed-.")
    good_versions = (0, 1, 2, 3)
    bad_schema_names = ("has_underscore", "caPitaLs",  "has a space", "illegal?chars*")
    bad_versions = (-1, -2, -3)

    for bad in bad_schema_names, v in good_versions
        @test_throws ArgumentError(expected_bad_name_message(bad)) Schema(bad, v)
        @test_throws ArgumentError(expected_bad_name_message(bad)) Schema("$bad@$v")
    end
    for good in good_schema_names, v in good_versions
        @test Schema(good, v) == Schema("$good@$v")
        @test Schema(good, v) == Schema{Symbol(good),v}()
    end
    for name in Iterators.flatten((bad_schema_names, good_schema_names)), bad in bad_versions
        @test_throws ArgumentError(expected_bad_version_message(bad)) Schema("$name@$bad")
        @test_throws ArgumentError(expected_bad_version_message(bad)) Schema(name, bad)
    end

    good_schema_identifiers, bad_schema_identifiers = generate_test_schema_identifiers(good_schema_names, bad_schema_names)

    for (good_child, good_full) in good_schema_identifiers
        @test Schema(good_child) == Schema(good_full)
    end
    for ((bad_string, expected_message), _) in bad_schema_identifiers
        @test_throws ArgumentError(expected_message) Schema(bad_string)
    end

    # for bad in bad_schema_identifiers


    # for good

    # # some utilities

    # declaration_parsing_error_message = (rec, enc) -> begin
    #     return """
    #            Error encountered attempting to parse first argument provided to `@schema`.
    #            Received: $rec
    #            Encountered: ArgumentError: $enc"""
    # end

    # for bad in ("has_underscore",
    #             "caPitaLs",
    #             "has a space",
    #             "illegal?chars*")
    #     @test_throws ArgumentError(bad_name_message(bad)) Schema(bad, 1)
    #     @test_throws ArgumentError(bad_id_message(bad)) Schema(bad)
    #     msg = declaration_parsing_error_message(bad, bad_id_message(bad))
    #     @eval @test_throws Legolas.SchemaDeclarationError($msg) @schema($bad, x)
    #     for expr in ("$bad@1",
    #                  Expr(:call, :(>), "$bad@1", "parent@1"),
    #                  Expr(:call, :(>), "child@1", "$bad@1"))
    #         msg = declaration_parsing_error_message(expr, bad_name_message(bad))
    #         @eval @test_throws Legolas.SchemaDeclarationError($msg) @schema($expr, x)
    #         @eval @test_throws Legolas.SchemaDeclarationError("no required fields declared") @schema($expr)
    #     end
    # end

    # for good in ("foo",
    #              "test.foo",
    #              "test.foo-bar",
    #              ".-technically-allowed-.")
    #     @test Schema(good, 1) == Schema("$good@1")
    #     @test Schema(good, 1) == Schema{Symbol(good), 1}()

    #     # @test_throws ArgumentError(bad_id_message(bad)) Schema(bad)
    #     # msg = declaration_parsing_error_message(bad, bad_id_message(bad))
    #     # @eval @test_throws Legolas.SchemaDeclarationError($msg) @schema($bad, x)
    #     # for expr in ("$bad@1",
    #     #             Expr(:call, :(>), "$bad@1", "parent@1"),
    #     #             Expr(:call, :(>), "child@1", "$bad@1"))
    #     #     msg = declaration_parsing_error_message(expr, bad_name_message(bad))
    #     #     @eval @test_throws Legolas.SchemaDeclarationError($msg) @schema($expr, x)
    #     # end
    # end

    # msg = declaration_parsing_error_message(bad, bad_name_message(bad))
    # msg = declaration_parsing_error_message(bad, bad_name_message(bad))
    # msg = declaration_parsing_error_message(bad, bad_name_message(bad))


    # bad = "Caps"
    # @test_throws ArgumentError(bad_name_message(bad)) Schema(bad, 1)
    # @test_throws ArgumentError(bad_id_message(bad)) Schema(bad)
    # for (expr, msg) in (bad,
    #                     "$bad@1",
    #                     Expr(:call, :(>), "$bad@1", "parent@1"),
    #                     Expr(:call, :(>), "child@1", "$bad@1"),
    #                     )

    # msg = declaration_parsing_error_message(bad, bad_id_message(bad))
    # @eval @test_throws Legolas.SchemaDeclarationError($msg) @schema($bad, x)
    # msg = declaration_parsing_error_message(bad, bad_id_message(bad))
    # @eval @test_throws Legolas.SchemaDeclarationError($msg) @schema($bad, x)



    # bad_id_msg = "argument is not a valid `Legolas.Schema` identifier: \"$bad\""
    # @test_throws ArgumentError(bad_id_msg) Schema(bad)

    # for (bad, earliest_problem) in ("has_underscore" => :name,
    #                                 "Caps" => :name,
    #                                 "question?" => :name,
    #                                 ">bad.prefix" => :identifier,
    #                                 "bad.suffix>" => :identifier,
    #                                 "oops.oops@-1" => :negative_version,
    #                                 "child.oops@1>parent.oops@1" => :identifier)

    #     bad_name_msg = "argument is not a valid `Legolas.Schema` name: \"$bad\""
    #     @test_throws ArgumentError(bad_name_msg) Schema(bad, 1)

    #     bad_id_msg = "argument is not a valid `Legolas.Schema` identifier: \"$bad\""
    #     @test_throws ArgumentError(bad_id_msg) Schema(bad)

    #     # for bad_expr in ("$bad_name@1",
    #     #                  Expr(:call, :(>), "$bad_name@1", "parent@1"),
    #     #                  Expr(:call, :(>), "child@1", "$bad_name@1"))
    #     #     dec_err_msg = declaration_parsing_error_message(bad_expr, "ArgumentError: $arg_err_msg")
    #     #     @eval @test_throws Legolas.SchemaDeclarationError($dec_err_msg) @schema($bad_expr, x)
    #     # end
    # end

    # for bad_id in bad_schema_names
    #     @test_throws ArgumentError("argument is not a valid `Legolas.Schema` name: \"$bad_id\"") Schema(bad_id, 1)
    #     arg_err_msg = "argument is not a valid `Legolas.Schema` identifier: \"$bad_id\""
    #     @test_throws ArgumentError(arg_err_msg) Schema(bad_id)
    #     for bad_expr in (bad_id,
    #                      Expr(:call, :(>), bad_id, "parent@1"),
    #                      Expr(:call, :(>), "child@1", bad_id))
    #         dec_err_msg = declaration_parsing_error_message(bad_expr, "ArgumentError: $arg_err_msg")
    #         @eval @test_throws Legolas.SchemaDeclarationError($dec_err_msg) @schema($bad_expr, x)
    #     end
    # end


    # msg =
    # @test_throws ArgumentError("argument is not a valid `Legolas.Schema` name: \"has_underscore\"") Schema("has_underscore", 1)
    # msg = declaration_parsing_error_message("", "ArgumentError: $msg")
    # @test_throws Legolas.SchemaDeclarationError($dec_err_msg) @schema($bad_expr, x)


    # @test_throws ArgumentError("argument is not a valid `Legolas.Schema` name: \"Caps\"") Schema("Caps", 1)
    # @test_throws ArgumentError("argument is not a valid `Legolas.Schema` name: \"question?\"") Schema("question?", 1)



    # msg = declaration_error_message("has_underscore", "ArgumentError: argument is not a valid `Legolas.Schema` name: \"has_underscore\"")
    # @test_throws(Legolas.SchemaDeclarationError(msg), @schema("has_underscore", x))

    # msg = declaration_error_message("Caps", "ArgumentError: argument is not a valid `Legolas.Schema` name: \"Caps\"")
    # @test_throws(Legolas.SchemaDeclarationError(msg), @schema("Caps", x))

    # msg = declaration_error_message("Caps", "ArgumentError: argument is not a valid `Legolas.Schema` name: \"Caps\"")
    # @test_throws(Legolas.SchemaDeclarationError(msg), @schema("Caps", x))

    # for bad_name in ("has_underscore",
    #                  "Caps",
    #                  "question?")
    #     @test_throws ArgumentError("argument is not a valid `Legolas.Schema` name: \"$bad_name\"") Schema(bad_name, 1)
    #     for bad_expr in ("$bad_name@1",
    #                      Expr(:call, :(>), "$bad_name@1", "parent@1"),
    #                      Expr(:call, :(>), "child@1", "$bad_name@1"))
    #         msg = "Error encountered attempting to parse first argument provided to `@schema`.\n" *
    #               "Received: $bad_expr\n" *
    #               "Encountered: ArgumentError: argument is not a valid `Legolas.Schema` name: \"$bad_name\""
    #         @eval @test_throws Legolas.SchemaDeclarationError($msg) @schema($bad_expr, x)
    #     end
    # end

    # for bad_string in (">bad.prefix",
    #                    "bad.suffix>",
    #                    "oops.oops@-1",
    #                    "child.oops@1>parent.oops@1")
    #     @test_throws ArgumentError("argument is not a valid `Legolas.Schema` string: \"$bad_string\"") Schema(bad_string)
    #     for bad_expr in (bad_string,
    #                      Expr(:call, :(>), bad_string, "parent@1"),
    #                      Expr(:call, :(>), "child@1", bad_string))
    #         msg = "Error encountered attempting to parse first argument provided to `@schema`.\n" *
    #               "Received: $bad_expr\n" *
    #               "Encountered: ArgumentError: argument is not a valid `Legolas.Schema` string: \"$bad_string\""
    #         @eval @test_throws Legolas.SchemaDeclarationError($msg) @schema($bad_expr, x)
    #     end
    # end

end


########################
########################
########################


@testset "miscellaneous Legolas/src/tables.jl tests" begin
    struct MyPath
        x::String
    end
    Base.read(p::MyPath) = Base.read(p.x)
    Base.write(p::MyPath, bytes) = Base.write(p.x, bytes)
    root = mktempdir()
    path = MyPath(joinpath(root, "baz.arrow"))
    Baz = @row("baz@1", a, b)
    t = [Baz(a=1, b=2), Baz(a=3, b=4)]
    Legolas.write(path, t, Schema("baz", 1))
    @test t == Baz.(Tables.rows(Legolas.read(path)))
    tbl = Arrow.Table(Legolas.tobuffer(t, Schema("baz", 1); metadata=("a" => "b", "c" => "d")))
    @test Set(Arrow.getmetadata(tbl)) == Set((Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "baz@1",
                                              "a" => "b", "c" => "d"))

    struct Foo
        meta
    end
    Legolas.Arrow.getmetadata(foo::Foo) = foo.meta
    foo = Foo(Dict("a" => "b", "b" => "b",
                   Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "baz@1"))
    @test Legolas.Schema("baz", 1) == Legolas.extract_legolas_schema(foo)

    t = [(a="a", c=1, b="b"), Baz(a=1, b=2)] # not a valid Tables.jl table
    @test_throws ErrorException Legolas.validate(t, Schema("baz", 1))

    t = Arrow.tobuffer((a=[1, 2], b=[3, 4]); metadata=Dict(Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "haha@3"))
    @test_throws Legolas.UnknownSchemaError Legolas.read(t)
end


@testset "miscellaneous Legolas.Schema tests" begin
    @test_throws ArgumentError("`Legolas.Schema` version must be non-negative, received: -1") Schema("good_name", -1)
    @test_throws ArgumentError("argument is not a valid `Legolas.Schema` name: \"bad_name?\"") Schema("bad_name?", 1)
    @test_throws ArgumentError("argument is not a valid `Legolas.Schema` string: \"bad_name>?@1\"") Schema("bad_name>?@1")

    @schema("foo@1", x, y)
    @schema("bar@1" > "foo@1", z)
    @test Legolas.schema_parent(Schema("bar", 1)) == Schema("foo", 1)

    r = apply(Schema("bar", 1), (x=1, y=2, z=3))

    @test propertynames(r) == (:z, :x, :y)
    @test r === apply(Schema("bar", 1), r)
    @test r === apply(Schema("bar", 1); x=1, y=2, z=3)
    @test r === apply(Schema("bar", 1), first(Tables.rows(Arrow.Table(Arrow.tobuffer((x=[1],y=[2],z=[3]))))))

    tbl = Arrow.Table(Arrow.tobuffer((x=[r],)))
    @test r === tbl.x[1]

    long_row = apply(Schema("bar", 1), (x=1, y=2, z=zeros(100, 100)))
    @test length(sprint(show, long_row; context=(:limit => true))) < 200

    @test_throws Legolas.UnknownSchemaError Legolas.apply(Legolas.Schema("imadethisup@3"); a=1, b=2)
    @test_throws Legolas.UnknownSchemaError Legolas.validate(Tables.Schema((:a, :b), (Int, Int)), Legolas.Schema("imadethisup@3"))
    @test_throws Legolas.UnknownSchemaError Legolas.schema_identifier(Legolas.Schema("imadethisup@3"))

    sch = Schema("bar", 1)
    @test Schema(sch) == sch

    schemas = [Schema("bar", 1), Schema("foo", 1)]
    tbl = Arrow.Table(Arrow.tobuffer((; schema=schemas)))
    @test all(tbl.schema .== schemas)
end

@testset "schema field name and type tests" begin
    Parent = @row("parent@1",
                  first_parent_field::Int=1,
                  second_parent_field::String="second")

    parent_fields = (:first_parent_field, :second_parent_field)
    parent_field_types = (Int, String)

    @test Legolas.schema_field_names(Schema{:parent,1}) == parent_fields
    @test Legolas.schema_field_names(Schema("parent@1")) == parent_fields
    @test Legolas.schema_field_names(Parent()) == parent_fields
    @test Legolas.schema_field_names(Parent) == parent_fields

    @test Legolas.schema_field_types(Schema{:parent,1}) == parent_field_types
    @test Legolas.schema_field_types(Schema("parent@1")) == parent_field_types
    @test Legolas.schema_field_types(Parent()) == parent_field_types
    @test Legolas.schema_field_types(Parent) == parent_field_types

    Child = @row("child@1" > "parent@1",
                 first_child_field::Symbol=:first,
                 second_child_field="I can be anything")

    child_fields = (:first_child_field, :second_child_field, parent_fields...)
    child_field_types = (Symbol, Any, parent_field_types...)

    @test Legolas.schema_field_names(Schema{:child,1}) == child_fields
    @test Legolas.schema_field_names(Schema("child@1")) == child_fields
    @test Legolas.schema_field_names(Child()) == child_fields
    @test Legolas.schema_field_names(Child) == child_fields

    @test Legolas.schema_field_types(Schema{:child,1}) == child_field_types
    @test Legolas.schema_field_types(Schema("child@1")) == child_field_types
    @test Legolas.schema_field_types(Child()) == child_field_types
    @test Legolas.schema_field_types(Child) == child_field_types

    @test_throws MethodError Legolas.schema_field_names(Legolas.Schema("imadethisup@3"))
    @test_throws MethodError Legolas.schema_field_types(Legolas.Schema("imadethisup@3"))
end

@testset "isequal, hash" begin
    TestRow = @row("testrow@1", x, y)

    foo = TestRow(; x=[1])
    foo2 = TestRow(; x=[1])
    @test isequal(foo, foo2)
    @test hash(foo) == hash(foo2)

    foo3 = TestRow(; x=[3])
    @test !isequal(foo, foo3)
    @test hash(foo) != hash(foo3)
end

@schema("my-inner-schema@1", b::Int=1)
@schema("my-outer-schema@1",
        a::String,
        x::NamedTuple=Legolas.row(Legolas.Schema("my-inner-schema@1"), x))

@testset "Nested arrow serialization" begin
    table = [(; a="outer_a", x=(; b=1))]
    roundtripped_table = Legolas.read(Legolas.tobuffer(table, Legolas.Schema("my-outer-schema@1")))
    @test table == (r -> Legolas.row(Legolas.Schema("my-outer-schema@1"), r)).(Tables.rows(roundtripped_table))
end
