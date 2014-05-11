open Async.Std
open Protocol
open AQueue
open Warmup

module Make = functor(Work : Ddwq.WorkType) -> struct

  let run work_input =
    (*(Sys.command "clear")
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
    >>= fun _ -> (debug INFO ("Finished initialization. # Active Slaves = " ^ (string_of_int (!num_alive_slaves)))); (after (Core.Std.sec 5.0)) >>= fun _ -> return () *)
    return (Work.run_and_package_work work_input)
end