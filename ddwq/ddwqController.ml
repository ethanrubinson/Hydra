open Async.Std
open Protocol
open AQueue
open Warmup
open Printexc

let alive_master_queue = AQueue.create ()
let num_alive_masters = ref 0
let current_leader = ref ""

let ips = ref []

(* Enable/Disable debug output for the Controller*)
let debug_mode_active = true
type debug_type = INFO | WARN | ERROR | FATAL | NONE
let debug t string_to_print = (let t_String = "\027[0m" ^ (match t with |INFO -> "\027[32m[INFO] >> " |WARN -> "\027[33m[WARN] >> "|ERROR -> "\027[31m[ERROR] >> "| FATAL -> "\027[31m\027[5m[FATAL] >> "|NONE -> "") in if debug_mode_active then (print_endline (t_String ^ string_to_print ^ "\027[0m" )) else ())


let init addrs = (ips := addrs)

module Make = functor(Work : Ddwq.WorkType) -> struct
  let print_slave ip port = (ip ^ ":" ^ string_of_int port)


  let run () =
    (Sys.command "clear")
    >>= fun _ ->

    (* Initialize connections to Slave nodes *)
    let slave_list = !ips in
    debug NONE "#########################";
    debug NONE "########  MASTER  #######";
    debug NONE "#########################";
    debug NONE "";
    debug NONE "#########################";
    debug NONE "## Installed Worktypes ##";
    debug NONE "#########################";
    debug NONE "##                     ##";
    List.iter (fun elem -> debug NONE ("##      "^ elem ^"      ##")) (Ddwq.list_worktypes ());
    debug NONE "##                     ##";
    debug NONE "#########################";
    debug NONE "";
    debug NONE "#########################";
    debug NONE "## Loaded Master Addrs ##";
    debug NONE "#########################";
    debug NONE "##                     ##";
    List.iter (fun elem -> debug NONE ( "##   " ^ (fst elem) ^ ":" ^ (string_of_int (snd elem)) ^ "   ##")) slave_list;
    debug NONE "##                     ##";
    debug NONE "#########################";
    debug NONE ""; 
    debug INFO ""

    let connect_and_initialize_master = fun (ip,port) -> begin
      (debug INFO ("Attempting to connect to Slave at " ^ print_slave ip port)); 
      try_with ( fun () -> (Tcp.connect ~timeout:(Core.Std.sec 1.0) (Tcp.to_host_and_port ip port) ) )
      >>= function
        | Core.Std.Result.Error e -> begin
          (debug WARN ("Failed to connect to Slave at " ^ print_slave ip port));
          return ()
        end
        | Core.Std.Result.Ok  v -> begin
          (debug INFO ("Connected to Slave at " ^ print_slave ip port));
          let (_,_,w) = v in 
          try_with (fun () -> return (Writer.write_line w Work.worktype_id))
          >>= function
            | Core.Std.Result.Error e -> begin
              (debug WARN ("Failed to initialize Slave at " ^ print_slave ip port));
              return ()
            end
            | Core.Std.Result.Ok    v' -> begin
              (debug INFO ("Initialized Slave at " ^ print_slave ip port));
              (num_alive_slaves := !num_alive_slaves + 1);
              return ( (push alive_queue v)) 
            end
        end
    end
    in
    Deferred.List.map ~how:`Parallel slave_list ~f:connect_and_initialize_slaves
    >>= fun _ -> (debug INFO ("Finished initialization. # Active Slaves = " ^ (string_of_int (!num_alive_slaves)))); (after (Core.Std.sec 5.0)) >>= fun _ -> return () 
end