(* One TCP connection to one broker.

   A mutex serialises request/response pairs, so there is at most one
   request in flight per connection; that is what keeps a partition's
   appends in order.

   Every round trip has a deadline. Any I/O failure, timeout or correlation
   mismatch leaves the byte stream at an unknown position (a partial frame
   may have been written, or a late response may still arrive), so the
   connection is closed and marked [broken] rather than reused. The
   [Router] notices and redials. *)

open Protocol

(* Bounds one request/response round trip. It must exceed the longest the
   broker may legitimately hold a request (a fetch long-poll, an acks=all
   wait, a JoinGroup waiting out a rebalance), so it is generous; its job is
   to turn a wedged broker into an error instead of a thread blocked
   forever. *)
let default_request_timeout_ms = 120_000
let default_dial_timeout_ms = 30_000
let default_client_id = "brahmaputra-ocaml"

type t = {
  fd : Unix.file_descr;
  address : string;
  client_id : string;
  mu : Mutex.t;
  mutable timeout_ms : int;
  mutable next : int32;
  broken : bool Atomic.t;
  closed : bool Atomic.t;
}

let with_lock mu f =
  Mutex.lock mu;
  Fun.protect ~finally:(fun () -> Mutex.unlock mu) f

(* A write to a socket the peer closed must be an EPIPE error, not a
   process-killing signal. *)
let ignore_sigpipe = lazy (try Sys.set_signal Sys.sigpipe Sys.Signal_ignore with _ -> ())

let split_address address =
  match String.rindex_opt address ':' with
  | None -> invalid_arg (Printf.sprintf "address %S is not host:port" address)
  | Some i ->
      let host = String.sub address 0 i in
      let host =
        if String.length host >= 2 && host.[0] = '[' then String.sub host 1 (String.length host - 2)
        else host
      in
      (host, String.sub address (i + 1) (String.length address - i - 1))

let conn_error fmt = Printf.ksprintf (fun msg -> raise (Connection_error msg)) fmt

let unix_message address (err, fn, _) =
  Printf.sprintf "%s: %s: %s" address fn (Unix.error_message err)

(* Waits until [fd] is readable (or writable); raises on the deadline. A
   deadline of [infinity] waits forever. *)
let wait_for fd ~write deadline what =
  let rec go () =
    let timeout =
      if deadline = infinity then -1.0
      else
        let remaining = deadline -. Unix.gettimeofday () in
        if remaining <= 0. then conn_error "%s timed out" what else remaining
    in
    match
      if write then Unix.select [] [ fd ] [] timeout else Unix.select [ fd ] [] [] timeout
    with
    | [], [], _ -> go ()
    | _ -> ()
    | exception Unix.Unix_error (Unix.EINTR, _, _) -> go ()
  in
  go ()

let dial ?(client_id = default_client_id) ?(dial_timeout_ms = default_dial_timeout_ms)
    ?(request_timeout_ms = default_request_timeout_ms) address =
  Lazy.force ignore_sigpipe;
  let host, port = split_address address in
  let ai =
    match Unix.getaddrinfo host port [ Unix.AI_SOCKTYPE Unix.SOCK_STREAM ] with
    | ai :: _ -> ai
    | [] -> conn_error "cannot resolve %s" address
    | exception Unix.Unix_error (e, f, a) -> raise (Connection_error (unix_message address (e, f, a)))
  in
  let fd = Unix.socket ai.Unix.ai_family Unix.SOCK_STREAM 0 in
  (try
     Unix.set_close_on_exec fd;
     (* Non-blocking for its whole life: every read and write waits in
        select against the round trip's deadline. *)
     Unix.set_nonblock fd;
     (try Unix.connect fd ai.Unix.ai_addr with
     | Unix.Unix_error ((Unix.EINPROGRESS | Unix.EWOULDBLOCK | Unix.EAGAIN | Unix.EINTR), _, _) ->
         let deadline =
           if dial_timeout_ms <= 0 then infinity
           else Unix.gettimeofday () +. (float_of_int dial_timeout_ms /. 1000.)
         in
         wait_for fd ~write:true deadline ("connect to " ^ address);
         match Unix.getsockopt_error fd with
         | None -> ()
         | Some err -> raise (Unix.Unix_error (err, "connect", address)));
     (* Responses are small and latency matters more than packet count. *)
     (try Unix.setsockopt fd Unix.TCP_NODELAY true with Unix.Unix_error _ -> ())
   with e ->
     (try Unix.close fd with Unix.Unix_error _ -> ());
     match e with
     | Unix.Unix_error (err, fn, arg) -> raise (Connection_error (unix_message address (err, fn, arg)))
     | e -> raise e);
  {
    fd;
    address;
    client_id;
    mu = Mutex.create ();
    timeout_ms = request_timeout_ms;
    next = 0l;
    broken = Atomic.make false;
    closed = Atomic.make false;
  }

(* Changes how long one round trip may take before the connection is
   abandoned. Zero or negative disables the bound. *)
let set_request_timeout_ms t ms = with_lock t.mu (fun () -> t.timeout_ms <- ms)

(* Whether this connection failed and must not be reused. *)
let broken t = Atomic.get t.broken

let address t = t.address

let close t =
  Atomic.set t.broken true;
  if Atomic.compare_and_set t.closed false true then
    try Unix.close t.fd with Unix.Unix_error _ -> ()

let deadline_of t =
  if t.timeout_ms <= 0 then infinity
  else Unix.gettimeofday () +. (float_of_int t.timeout_ms /. 1000.)

let rec write_all t s off len deadline =
  if len > 0 then begin
    wait_for t.fd ~write:true deadline "request";
    match Unix.single_write_substring t.fd s off len with
    | n -> write_all t s (off + n) (len - n) deadline
    | exception Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _) ->
        write_all t s off len deadline
  end

let read_exact t n deadline =
  let buf = Bytes.create n in
  let rec go off =
    if off < n then begin
      wait_for t.fd ~write:false deadline "response";
      match Unix.read t.fd buf off (n - off) with
      | 0 -> conn_error "%s closed the connection" t.address
      | got -> go (off + got)
      | exception Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _) -> go off
    end
  in
  go 0;
  Bytes.unsafe_to_string buf

let read_frame t deadline =
  let header = read_exact t 4 deadline in
  let length = Int32.to_int (String.get_int32_be header 0) in
  if length < 0 then conn_error "negative frame length %d" length;
  read_exact t length deadline

(* Marks the connection unusable and re-raises the failure as a
   [Connection_error] (keeping a decode error's own type). *)
let fail t e =
  close t;
  match e with
  | Connection_error _ | Decode_error _ -> raise e
  | Unix.Unix_error (err, fn, arg) -> raise (Connection_error (unix_message t.address (err, fn, arg)))
  | e -> raise e

let broken_error t =
  Connection_error (Printf.sprintf "connection to %s is broken; the router will redial" t.address)

(* Sends one request and returns the matching response body. *)
let request t api_key body =
  with_lock t.mu (fun () ->
      if Atomic.get t.broken then raise (broken_error t);
      t.next <- Int32.succ t.next;
      let correlation_id = t.next in
      let deadline = deadline_of t in
      try
        let frame = encode_frame api_key correlation_id t.client_id body in
        write_all t frame 0 (String.length frame) deadline;
        (* A timeout here means the response may still be on its way, and
           reading on would pair it with the next request. *)
        let got, response = decode_frame_payload (read_frame t deadline) in
        if got <> correlation_id then
          conn_error "correlation id mismatch: expected %ld, got %ld" correlation_id got;
        response
      with e -> fail t e)

(* Sends without awaiting a response (acks=0). *)
let send_oneway t api_key body =
  with_lock t.mu (fun () ->
      if Atomic.get t.broken then raise (broken_error t);
      t.next <- Int32.succ t.next;
      let deadline = deadline_of t in
      try
        let frame = encode_frame api_key t.next t.client_id body in
        write_all t frame 0 (String.length frame) deadline
      with e -> fail t e)

type api_version_range = { api_key : int32; min_version : int32; max_version : int32 }

(* What the broker speaks, and its version string. This is the one call
   that works across a version mismatch. *)
let api_versions t =
  let w = Writer.body () in
  Writer.string w "brahmaputra-ocaml";
  Writer.string w "0.1.0";
  let r = Reader.body (request t api_api_versions (Writer.contents w)) in
  let code = Reader.int32 r in
  if code <> err_none then raise (server_error code "api_versions");
  let ranges =
    Reader.list r (fun r ->
        let api_key = Reader.int32 r in
        let min_version = Reader.int32 r in
        let max_version = Reader.int32 r in
        { api_key; min_version; max_version })
  in
  (ranges, Reader.string r)
