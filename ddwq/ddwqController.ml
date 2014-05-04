open Async.Std
open Protocol
open AQueue
open Warmup

let m  = Mutex.create()
let alive_queue = AQueue.create ()
let num_alive_slaves = ref 0
let ips = ref []

let init addrs =
  (Mutex.lock m);
  (ips := addrs);
  Mutex.unlock m

module Make = functor(Work : Ddwq.WorkType) -> struct
  let print_slave ip port = (ip ^ ":" ^ string_of_int port)


  let run () =
    (* Initialize connections to Slave nodes *)
    (Mutex.lock m);
    let slave_list = !ips in
    (Mutex.unlock m);

    
    let connect_and_initialize_slaves = fun (ip,port) ->
      (print_endline ("[INFO] Attempting to connect to Slave at " ^ print_slave ip port));
      try_with ( fun () -> (Tcp.connect (Tcp.to_host_and_port ip port)) )
      >>= function
        | Core.Std.Result.Error e -> (print_endline ("[ERROR] Failed to connect to Slave at " ^ print_slave ip port));
                       return ()
        | Core.Std.Result.Ok  v -> ((print_endline ("[INFO] Connected to Slave at " ^ print_slave ip port));
                       let (_,_,w) = v in 
                         try_with (fun () -> return (Writer.write_line w Work.worktype_id))
                         >>= function
                             | Core.Std.Result.Error e -> (print_endline ("[ERROR] Failed to initialize Slave at " ^ print_slave ip port));
                                            return ()
                             | Core.Std.Result.Ok    v' -> (print_endline ("[INFO] Initialized Slave at " ^ print_slave ip port));
                                             (num_alive_slaves := !num_alive_slaves + 1);
                                             return (push alive_queue v) )
    in deferred_map slave_list connect_and_initialize_slaves
    >>= fun _ -> return ()
end