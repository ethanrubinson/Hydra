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
      +> flag "-ip"(optional_with_default "localhost" string)
        ~doc:"IP/Hostname of head in the chain"
      +> flag "-portsend" (optional_with_default 20000 int)
        ~doc:"Listening port of head in the chain"
      +> flag "-portreceive" (optional_with_default 30000 int)
        ~doc:"Listening port of head in the chain"

    )
    (fun ip port_send port_receive () ->

       let cmd = ref "" in
       let rec do_client_stuff () = 
       (Sys.command "clear")
       >>= fun _ ->

debug NONE "";
debug NONE "  8888888b.      8888888b.      888       888      .d88888b.";      
debug NONE "  888  \"Y88b     888  \"Y88b     888   o   888     d88P\" \"Y88b";     
debug NONE "  888    888     888    888     888  d8b  888     888     888";    
debug NONE "  888    888     888    888     888 d888b 888     888     888";     
debug NONE "  888    888     888    888     888d88888b888     888     888";     
debug NONE "  888    888     888    888     88888P Y88888     888 Y8b 888";     
debug NONE "  888  .d88P d8b 888  .d88P d8b 8888P   Y8888 d8b Y88b.Y8b88P d8b"; 
debug NONE "  8888888P\"  Y8P 8888888P\"  Y8P 888P     Y888 Y8P  \"Y888888\"  Y8P"; 
debug NONE "                                                       Y8b";      
debug NONE "";                                                                
debug NONE "";                                                                
debug NONE "            .d8888b.  888 d8b                   888";                  
debug NONE "           d88P  Y88b 888 Y8P                   888";                  
debug NONE "           888    888 888                       888";                  
debug NONE "           888        888 888  .d88b.  88888b.  888888";               
debug NONE "           888        888 888 d8P  Y8b 888 \"88b 888";                  
debug NONE "           888    888 888 888 88888888 888  888 888";                  
debug NONE "           Y88b  d88P 888 888 Y8b.     888  888 Y88b.";                
debug NONE "            \"Y8888P\"  888 888  \"Y8888  888  888  \"Y888";
debug NONE "";
debug NONE "";
debug NONE "   /= = = = = = = = = = = = = = = = = = = = = = = = = = = = =\\";
debug NONE "  /= = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\\";
debug NONE " ||                                                           ||";
debug NONE " ||                   Installed Work Modules                  ||";
debug NONE " ||                                                           ||";
debug NONE " ||            » work.addi      Adds two integers             ||";
debug NONE " ||            » work.exec      Runs an OCaml source          ||";
debug NONE " ||            » work.full      Relays a message              ||";
debug NONE " ||                                                           ||";
debug NONE "  \\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = =/";
debug NONE "   \\= = = = = = = = = = = = = = = = = = = = = = = = = = = = =/";


         debug NONE "";
         debug NONE "Specify a 'worktype' identifier from the list below (\"quit\" to exit) :\n";
         debug NONE (WorktypeList.list_worktypes());

         let work_name = Pervasives.input_line stdin in
         cmd := work_name;
         if not (!cmd = "quit") then begin 
           let m = match (Ddwq.get_worktype_for_id work_name) with |Some x -> x | None -> failwith "No such 'worktype'"in
           let module MyWork = (val m) in

           debug NONE "[s]end OR [r]eceive : ";
           let s_r = Pervasives.input_line stdin in



           if s_r = "s" then begin 
             debug NONE "Enter a user ID to register the work under : ";
             let user_id = Pervasives.input_line stdin in

             debug NONE "Please specify a unique work identification number : ";
             let work_id = Pervasives.input_line stdin in

             debug NONE "Launch work for string input : ";
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

                 debug INFO "Connected to the DDWQ.";

                 debug INFO "Relaying 'worktype' identifier to being initialization.";

                 CIReq.send w (CIReq.InitForWorkType(user_id,int_of_string work_id,work_name));

                 debug INFO "Waiting on initialization response.";
                 CIRes.receive r
                 >>= function
                 | `Eof  -> begin 
                     debug ERROR "Got an error while waiting for initialization response!";
                     return()
                   end
                 | `Ok msg -> begin 
                     match msg with
                     | CIRes.InitForWorkTypeSucceeded -> begin 
                         debug INFO "Successfully initialized for 'worktype' request. Sending our work to the DDWQ!";
                         CReq.send w (CReq.DDWQWorkRequest(MyWork.load_input_for_string work_cmd));
                         debug INFO "Work has been sent successfully.";
                         return()
                       end 
                     | CIRes.InitForWorkTypeFailed f -> begin 
                         debug ERROR f;
                         return()
                       end
                   end

               end
               >>= fun _ -> (after (Core.Std.sec 5.0)) >>= fun _ -> do_client_stuff()
           end

           else begin 
             debug NONE "Enter the user ID the work request was registered under : ";
             let user_id = Pervasives.input_line stdin in

             debug NONE "Enter corresponding work identification number : ";
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

                 debug INFO "Connected to the DDWQ.";

                 debug INFO "Relaying 'worktype' identifier to being initialization.";

                 CIReq.send w (CIReq.InitForWorkType(user_id,int_of_string work_id,work_name));

                 debug INFO "Waiting on initialization response";
                 CIRes.receive r
                 >>= function
                 | `Eof  -> begin 
                     debug ERROR "Got an error while waiting for initialization response!";
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
                                     debug INFO ("Retreived completed work the result in string format is below: \nRESULT: " ^ (MyWork.work_output_to_string x));
                                     return()
                                   end
                                 | None -> begin 
                                     debug WARN "No completed work was found for the specified user work identification.\nYou may want to try again in a minute.";
                                     return()

                                   end

                               end

                           end

                       end 
                     | CIRes.InitForWorkTypeFailed f -> begin 
                         debug ERROR f;
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