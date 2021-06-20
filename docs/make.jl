using Legolas
using Documenter

makedocs(modules=[Legolas],
         sitename="Legolas",
         authors="Beacon Biosignals, Inc.",
         pages=["API Documentation" => "index.md",
                "Tips For Schema Authors" => "schema.md",
                "FAQ" => "faq.md"])

deploydocs(repo="github.com/beacon-biosignals/Legolas.jl.git",
           push_preview=true,
           devbranch="main")
