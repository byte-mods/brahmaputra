module Brahmaputra.FSharp.ManualTest.Proxy

open System
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks

/// Forwards TCP to the broker and can sever every live connection, which is
/// how a broker restart or an idle timeout looks to a client.
[<Sealed>]
type TcpProxy(target: string) =
    let colon = target.LastIndexOf ':'
    let host = target.Substring(0, colon)
    let port = int (target.Substring(colon + 1))
    let listener = new TcpListener(IPAddress.Loopback, 0)
    let live = ResizeArray<TcpClient>()

    let pipe (source: TcpClient) (sink: TcpClient) : Task =
        task {
            try
                do! source.GetStream().CopyToAsync(sink.GetStream())
            with _ ->
                () // either side closed

            sink.Dispose()
        }

    do
        listener.Start()

        Task.Run(fun () ->
            task {
                let mutable running = true

                while running do
                    let! accepted =
                        task {
                            try
                                let! c = listener.AcceptTcpClientAsync()
                                return Some c
                            with _ ->
                                return None
                        }

                    match accepted with
                    | None -> running <- false
                    | Some client ->
                        let upstream = new TcpClient()

                        let! connected =
                            task {
                                try
                                    do! upstream.ConnectAsync(host, port)
                                    return true
                                with _ ->
                                    return false
                            }

                        if not connected then
                            client.Dispose()
                            upstream.Dispose()
                        else
                            lock live (fun () ->
                                live.Add client
                                live.Add upstream)

                            pipe client upstream |> ignore
                            pipe upstream client |> ignore
            }
            :> Task)
        |> ignore

    /// The address clients should dial.
    member _.Address = sprintf "127.0.0.1:%d" (listener.LocalEndpoint :?> IPEndPoint).Port

    /// Closes every live connection, both directions.
    member _.DropAll() =
        lock live (fun () ->
            for c in live do
                c.Dispose()

            live.Clear())

        Thread.Sleep 50

    interface IDisposable with
        member this.Dispose() =
            listener.Stop()
            this.DropAll()

/// Accepts connections and never answers.
[<Sealed>]
type SilentBroker() =
    let listener = new TcpListener(IPAddress.Loopback, 0)
    let held = ResizeArray<TcpClient>()

    do
        listener.Start()

        Task.Run(fun () ->
            task {
                try
                    while true do
                        let! c = listener.AcceptTcpClientAsync()
                        lock held (fun () -> held.Add c)
                with _ ->
                    () // listener stopped
            }
            :> Task)
        |> ignore

    member _.Address = sprintf "127.0.0.1:%d" (listener.LocalEndpoint :?> IPEndPoint).Port

    interface IDisposable with
        member _.Dispose() =
            listener.Stop()

            lock held (fun () ->
                for c in held do
                    c.Dispose())
