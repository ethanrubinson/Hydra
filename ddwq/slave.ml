open Async.Std
open Protocol
open Unix

module Make (Work : Ddwq.WorkType) = struct

  let rec run r w =
     let module SlaveReq = SlaveRequest(Work) in
     let module SlaveRes = SlaveResponse(Work) in
     (print_endline "[INFO] >> Waiting for work data...");
     SlaveReq.receive r
     >>= fun req ->
      match req with
        | `Eof ->  (print_endline "[WARN] >> Socket was closed. Terminating session"); (print_endline "");  (Reader.close r)
        | `Ok (SlaveReq.DoWorkRequest(i)) -> (print_endline "[INFO] >> Got some work data. Doing work...");
            (try_with (fun () -> Work.run_work i) >>= fun res ->
              match res with
     			      |Core.Std.Result.Error e -> (print_endline "[ERROR] >> Work failed. Sending failure notice.");
                                            (SlaveRes.send w (SlaveRes.DoWorkFailed(Core.Error.to_string_hum (Core.Error.of_exn e)))); run r w
     			      |Core.Std.Result.Ok v ->  (print_endline "[INFO] >> Work succeeded. Sending result.");
                                          (SlaveRes.send w (SlaveRes.DoWorkResult(v))); run r w)

end

(* see .mli *)
let init port =
  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port port)
    (fun _ r w ->

      print_endline "[INFO] >> Connection established. Session started.";
      print_endline "[INFO] >> Waiting for work initialization request...";
      Reader.read_line r >>= function
        | `Eof    -> (print_endline "[WARN] >> Socket was closed. Terminating session."); (print_endline ""); return ()
        | `Ok work_id -> (print_endline "[INFO] >> Got initialization packet."); match Ddwq.get_worktype_for_id work_id with
          | None -> (print_endline ("[WARN] >> No installed work modules have type : " ^ work_id ^". Terminating session.")); (print_endline ""); return ()
          | Some wrk ->
            let module Work = (val wrk) in
            (print_endline ("[INFO] >> Loaded work module of type: " ^ Work.worktype_id ^ "." ));
            let module DoWork = Make(Work) in
            DoWork.run r w
    )
    >>= fun _ ->
    (Sys.command "clear")
    >>= fun _ ->
    print_endline "#####################";
    print_endline "## DDWQ Slave Node ##";
    print_endline "#####################";
    print_endline "";
    print_endline ("## Host : " ^ (gethostname()));
    print_endline ("## Port : " ^ (string_of_int port));
    print_endline "";
    print_endline "##########################";
    print_endline "## Installed Work-Types ##";
    print_endline "##########################";
    print_endline "##                      ##";
    List.iter (fun elem -> print_endline ("##      "^ elem ^"       ##")) (Ddwq.list_worktypes ());
    print_endline "##                      ##";
    print_endline "##########################";
    print_endline "";
    print_endline "[INFO] >> Slave node started.";
    print_endline "[INFO] >> Waiting for connection from Master...";
    (print_endline ""); 

    never ()


