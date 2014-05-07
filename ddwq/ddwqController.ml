open Async.Std
open Protocol
open AQueue
open Warmup
open Printexc

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
    
    let slave_list = !ips in
   
    let index = ref 0 in 
    
    let connect_and_initialize_slaves = fun (ip,port) ->
      (print_endline ("[INFO] Attempting to connect to Slave at " ^ print_slave ip port)); 
      try_with (fun () -> ( (after (Core.Std.sec 3.0)) >>= fun _ -> (print_endline (string_of_int !index)); (index := !index + 1); return !index)) 
      >>= function 
      |Core.Std.Result.Error e -> (print_endline ("failed with error")); (return ())
      |Core.Std.Result.Ok v -> (print_endline ("DONE" ^ string_of_int !index) ); (return ())
            (*
      try_with ( fun () -> (Tcp.connect (Tcp.to_host_and_port ip port) ) )
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
                                             
                                             (Mutex.lock m);
                                             (num_alive_slaves := !num_alive_slaves + 1);
                                             (Mutex.unlock m);

                                             return ( (Mutex.lock m); (push alive_queue v); (Mutex.unlock m)) )
    *)in
    Deferred.List.map ~how:`Parallel slave_list ~f:connect_and_initialize_slaves
    >>= fun _ -> (print_endline (string_of_int (!num_alive_slaves))); (after (Core.Std.sec 10.0) >>= fun _ -> return () )
end