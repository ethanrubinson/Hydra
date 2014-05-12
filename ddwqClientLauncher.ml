open Async.Std
open Ddwq
open Protocol
open Debug


let () =
  Command.async_basic
    ~summary:"Run the DDWQ Client Interface"
    ~readme:WorktypeList.list_worktypes
    Command.Spec.(
      empty
      +> flag "-ip"(required string)
         ~doc:"IP/Hostname of head in the chain"
      +> flag "-portsend" (optional_with_default 20000 int)
         ~doc:"Listening port of head in the chain"
      +> flag "-portreceive" (optional_with_default 30000 int)
         ~doc:"Listening port of head in the chain"
      
    )
    (fun ip port_send port_receive () ->
      
      let cmd = ref "" in
      let rec do_client_stuff () = 
        debug NONE "Enter WorkType identifier ex. \"work.full\" or \"quit\" to exit: ";
        let work_name = Pervasives.input_line stdin in
        cmd := work_name;
        if not (!cmd = "quit") then begin 
          let m = match (Ddwq.get_worktype_for_id work_name) with |Some x -> x | None -> failwith "Worktype not loaded" in
          let module MyWork = (val m) in

          debug NONE "[s]end or [r]eceive : ";
          let s_r = Pervasives.input_line stdin in



          if s_r = "s" then begin 
            debug NONE "Enter user ID (Cannot contain \"|\" character): ";
            let user_id = Pervasives.input_line stdin in

            debug NONE "Enter a unique work ID#: ";
            let work_id = Pervasives.input_line stdin in

            debug NONE "Work command: ";
            let work_cmd = Pervasives.input_line stdin in



            try_with ( fun () -> (Tcp.connect (Tcp.to_host_and_port ip port_send)) )
                >>= function
                | Core.Std.Result.Error e -> begin 
                  debug ERROR ("Failed to connect to chain");
                  return()
                end
                | Core.Std.Result.Ok m -> begin

                  (* Finish initializing the state with info about the connection *)
                  let (a,r,w) = m in
                  (*!state.next_node := Some((a,r,w),(ip,port));*) (*Change... see below*)
                  let module CIReq = ClientInitRequest in
                  let module CIRes = ClientInitResponse in
                  let module CReq = ClientRequest(MyWork) in
                  let module CRes = ClientResponse(MyWork) in

                  debug INFO "Connected to the chain.";

                  debug INFO "Sending worktype identifier.";

                  CIReq.send w (CIReq.InitForWorkType(user_id,int_of_string work_id,work_name));

                  debug INFO "Waiting on response";
                  CIRes.receive r
                  >>= function
                    | `Eof  -> begin 
                      debug INFO "Got an error while waiting for response!";
                      return()
                    end
                    | `Ok msg -> begin 
                      match msg with
                        | CIRes.InitForWorkTypeSucceeded -> begin 
                          debug INFO "Successfully initialized work request. Sending it now!";
                          CReq.send w (CReq.DDWQWorkRequest(MyWork.load_input_for_string work_cmd));
                          return()
                        end 
                        | CIRes.InitForWorkTypeFailed f -> begin 
                          debug INFO f;
                          return()
                        end
                    end

                end
                >>= fun _ -> (after (Core.Std.sec 5.0)) >>= fun _ -> do_client_stuff()
          end

          else begin 
            debug NONE "Enter the user ID the work was registered under: ";
            let user_id = Pervasives.input_line stdin in

            debug NONE "Enter the registered ID#: ";
            let work_id = Pervasives.input_line stdin in



            try_with ( fun () -> (Tcp.connect (Tcp.to_host_and_port ip port_receive)) )
            >>= function
              | Core.Std.Result.Error e -> begin 
                  debug ERROR ("Failed to connect to chain");
                  return()
              end
              | Core.Std.Result.Ok m -> begin

                  (* Finish initializing the state with info about the connection *)
                  let (a,r,w) = m in
                  (*!state.next_node := Some((a,r,w),(ip,port));*) (*Change... see below*)
                  let module CIReq = ClientInitRequest in
                  let module CIRes = ClientInitResponse in
                  let module CReq = ClientRequest(MyWork) in
                  let module CRes = ClientResponse(MyWork) in

                  debug INFO "Connected to the chain.";

                  debug INFO "Sending worktype identifier.";

                  CIReq.send w (CIReq.InitForWorkType(user_id,int_of_string work_id,work_name));

                  debug INFO "Waiting on response";
                  CIRes.receive r
                  >>= function
                    | `Eof  -> begin 
                      debug ERROR "Got an error while waiting for response!";
                      return()
                    end
                    | `Ok msg -> begin 
                      match msg with
                        | CIRes.InitForWorkTypeSucceeded -> begin 
                          debug INFO "Successfully sent work retreival request. Waiting on response";
                          CRes.receive r
                          >>= function
                            | `Eof-> begin  
                              debug ERROR "Got an error while waiting for response!";
                              return()
                            end

                            | `Ok msg -> begin 
                              match msg with 
                                | CRes.DDWQWorkResult(work_result) -> begin  
                                  match work_result with 
                                    | Some x -> begin 
                                      debug INFO ("Retreived completed work!\nRESULT=" ^ (MyWork.work_output_to_string x));
                                      return()
                                    end
                                    | None -> begin 
                                      debug WARN "No completed work was found for given user and ID #!";
                                      return()

                                    end
                                  
                                end
                            
                            end

                        end 
                        | CIRes.InitForWorkTypeFailed f -> begin 
                          debug INFO f;
                          return()
                        end
                    end

                end
                >>= fun _ -> (after (Core.Std.sec 5.0)) >>= fun _ -> do_client_stuff()
          end


        end
      else begin  return() end

      in
      do_client_stuff()


      >>| fun () -> (shutdown 0)
    )
  |> Command.run