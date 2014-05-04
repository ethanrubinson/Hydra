open Async.Std

type 'a t = 'a Pipe.Reader.t * 'a Async.Std.Pipe.Writer.t

let create () =
  Async.Std.Pipe.create ()

let push (q: 'a t) (x : 'a) : unit =
  don't_wait_for(Async.Std.Pipe.write (snd q) x)

let pop  q =
  Async.Std.Pipe.read (fst q) >>= function
	| `Ok msg -> return msg
  	| `Eof -> failwith "Error: Queue invalid"
