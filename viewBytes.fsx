open System.IO

let breaks () =
    File.ReadAllBytes("stream0.puro")
    |> Array.map (fun b -> if b < 128uy then int b else int b - 256)
    |> Array.chunkBySize 40
    |> Array.map (fun a -> a |> Array.map string |> String.concat ", ")
    |> Array.map (fun line -> printfn "%s" line)
    |> ignore

let singleLine () =
    File.ReadAllBytes("stream0.puro")
    |> Array.map (fun b -> if b < 128uy then int b else int b - 256)
    |> Array.map string
    |> String.concat ", "
    |> printfn "%s"
    |> ignore

let rangeWithOffsets (startByte: int) (endByte: int) =
    let bytes = File.ReadAllBytes("stream0.puro")
    let relevantBytes = bytes.[startByte..endByte]

    relevantBytes
    |> Array.mapi (fun i b -> (i + startByte, (if b < 128uy then int b else int b - 256)))
    |> Array.chunkBySize 20
    |> Array.iter (fun chunk ->
        printfn "%s" (String.replicate 105 "-")

        printf "VAL: "

        chunk
        |> Array.map (fun (_, b) -> sprintf "%4d" b)
        |> String.concat " "
        |> printfn "%s"

        printf "GLB: "

        chunk
        |> Array.map (fun (globalIdx, _) -> sprintf "%4d" globalIdx)
        |> String.concat " "
        |> printfn "%s"

        printf "REL: "

        chunk
        |> Array.map (fun (globalIdx, _) ->
            let relativeIdx = globalIdx - startByte
            sprintf "%4d" relativeIdx)
        |> String.concat " "
        |> printfn "%s")

    printfn "%s" (String.replicate 105 "-")


let main (argv: string array) =
    let mode = if argv.Length > 1 then argv.[1] else "default"

    match mode with
    | "default" -> breaks ()
    | "s" -> singleLine ()
    | "r" ->
        let start = if argv.Length > 2 then int argv.[2] else 0
        let endIdx = if argv.Length > 3 then int argv.[3] else 79
        rangeWithOffsets start endIdx
    | _ -> failwith "Illegal arg"

    0

let args = fsi.CommandLineArgs
main args
