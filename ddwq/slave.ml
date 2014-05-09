open Async.Std
open Protocol
open Unix


let debug_mode_active = true
let debug string_to_print = (if debug_mode_active then (print_endline string_to_print) else ())

module Make (Work : Ddwq.WorkType) = struct

  let rec run r w =
     let module SlaveReq = SlaveRequest(Work) in
     let module SlaveRes = SlaveResponse(Work) in
     (debug "[INFO] >> Waiting for work data...");
     SlaveReq.receive r
     >>= fun req ->
      match req with
        | `Eof ->  (debug "[WARN] >> Socket was closed. Terminating session"); (debug "");  (Reader.close r)
        | `Ok (SlaveReq.DoWorkRequest(i)) -> (debug "[INFO] >> Got some work data. Doing work...");
            (try_with (fun () -> Work.run_work i) >>= fun res ->
              match res with
     			      |Core.Std.Result.Error e -> (debug "[ERROR] >> Work failed. Sending failure notice.");
                                            (SlaveRes.send w (SlaveRes.DoWorkFailed(Core.Error.to_string_hum (Core.Error.of_exn e)))); run r w
     			      |Core.Std.Result.Ok v ->  (debug "[INFO] >> Work succeeded. Sending result.");
                                          (SlaveRes.send w (SlaveRes.DoWorkResult(v))); run r w)

end

(* see .mli *)
let init port =
  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port port)
    (fun _ r w ->

      debug "[INFO] >> Connection established. Session started.";
      debug "[INFO] >> Waiting for work initialization request...";
      Reader.read_line r >>= function
        | `Eof    -> (debug "[WARN] >> Socket was closed. Terminating session."); (debug ""); return ()
        | `Ok work_id -> (debug "[INFO] >> Got initialization packet."); match Ddwq.get_worktype_for_id work_id with
          | None -> (debug ("[WARN] >> No installed work modules have type : " ^ work_id ^". Terminating session.")); (debug ""); return ()
          | Some wrk ->
            let module Work = (val wrk) in
            (debug ("[INFO] >> Loaded work module of type: " ^ Work.worktype_id ^ "." ));
            let module DoWork = Make(Work) in
            DoWork.run r w
    )
    >>= fun _ ->
    (Sys.command "clear")
    >>= fun _ ->
    debug "#####################";
    debug "## DDWQ Slave Node ##";
    debug "#####################";
    debug "";
    debug ("## Host : " ^ (gethostname()));
    debug ("## Port : " ^ (string_of_int port));
    debug "";
    debug "##########################";
    debug "## Installed Work-Types ##";
    debug "##########################";
    debug "##                      ##";
    List.iter (fun elem -> debug ("##      "^ elem ^"       ##")) (Ddwq.list_worktypes ());
    debug "##                      ##";
    debug "##########################";
    debug "";
    debug "[INFO] >> Slave node started.";
    debug "[INFO] >> Waiting for connection from Master...";
    debug ""; 

    never ()


