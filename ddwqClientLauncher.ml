open Async.Std
open Ddwq
open Protocol
open Debug

type ip = string
type port = int
type listen_node    = (ip * port) option ref
type response_node  = (ip * port) option ref

let print_main_screen () = 
  debug NONE 
"             888    888               888                 
             888    888               888                 
             888    888               888                 
             8888888888 888  888  .d88888 888d888 8888b.  
             888    888 888  888 d88\" 888 888P\"      \"88b
             888    888 888  888 888  888 888    .d888888
             888    888 Y88b 888 Y88b 888 888    888  888
             888    888  \"Y88888  \"Y88888 888    \"Y888888
                            888                          
                       Y8b d88P                          
                        \"Y88P\"               
                                                     
                                                     
              .d8888b.  888 d8b                   888
             d88P  Y88b 888 Y8P                   888
             888    888 888                       888
             888        888 888  .d88b.  88888b.  888888
             888        888 888 d8P  Y8b 888 \"88b 888
             888    888 888 888 88888888 888  888 888
             Y88b  d88P 888 888 Y8b.     888  888 Y88b.
              \"Y8888P\"  888 888  \"Y8888  888  888  \"Y888


     /= = = = = = = = = = = = = = = = = = = = = = = = = = = = =\\
    /= = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\\
   ||                                                           ||
   ||                   Installed Work Modules                  ||
   ||                                                           ||
   ||            » work.addi      Adds two integers             ||
   ||            » work.exec      Runs an OCaml source          ||
   ||            » work.full      Relays a message              ||
   ||                                                           ||
    \\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = =/
     \\= = = = = = = = = = = = = = = = = = = = = = = = = = = = =/
              
"


let head_node : listen_node   = ref None
let tail_node : response_node = ref None

let get_node_ip node =
  match node with
    | None -> failwith  "Node is None"
    | Some(nodeinfo) -> fst nodeinfo

let get_node_port node =
  match node with
    | None -> failwith  "Node is None"
    | Some(nodeinfo) -> snd nodeinfo

let close_socket_and_do_func m f = 
  (Socket.shutdown m `Both);
  head_node := None;
  tail_node := None;
  ((after (Core.Std.sec (Random.float 5.0))) >>= fun _ -> f())

let rec get_where_to_connect_from_master master_ip master_port () = 
  debug INFO "Attempting to connect to Hydra's Master-Service...";
  try_with ( fun () -> (Tcp.connect ~timeout:(Core.Std.sec 0.1) (Tcp.to_host_and_port master_ip master_port)) )
  >>= function
  | Core.Std.Result.Error e -> begin 
      debug ERROR "Failed to connect to Hydra's Master-Service. Retry in 5 seconds";
      ((after (Core.Std.sec 5.0)) >>= fun _ -> (get_where_to_connect_from_master master_ip master_port ()))
    end
  | Core.Std.Result.Ok m -> begin
      debug INFO "Connected!";

      let (a,r,w) = m in
      
      let module ClReq = ClientToMasterRequest in
      let module ClRes = ClientToMasterResponse in

      debug INFO "Sending our initialization request.";
      ClReq.send w ClReq.HeadAndTailRequest;

      debug INFO "Waiting for resopnse...";
      ClRes.receive r
      >>= function
      | `Eof -> begin
          debug ERROR "Failed to receive initialization response. Retrying in 5 seconds";
          close_socket_and_do_func a (get_where_to_connect_from_master master_ip master_port)
        end

      | `Ok init_response -> begin
          match init_response with 
          | ClRes.HeadAndTailResponse(res) -> begin

              match res with 
                | Some(node_info) -> begin
                  let (head_info,tail_info) = node_info in
                    debug INFO "Seccessfully retreived Head and Tail connection information.";
                    (head_node := Some(head_info));
                    (tail_node := Some(tail_info));
                    return()
                end
                | None -> begin 
                  debug WARN "Master says there is no chain. Retrying in 5 seconds";
                  close_socket_and_do_func a (get_where_to_connect_from_master master_ip master_port)
                end
          end 
      end
    end


let rec start_client m_ip m_port () = 
  Sys.command "clear" >>= fun _ ->
  print_main_screen ();

  debug NONE "Specify a 'worktype' identifier from the list above [\"quit\" to exit] :\n";

  let cmd = ref "" in
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

      (*Begin process for sending the work to the head node*)
      try_with ( fun () -> (Tcp.connect (Tcp.to_host_and_port (get_node_ip !head_node) (get_node_port !head_node))))
      >>= function
        | Core.Std.Result.Error e -> begin 
          debug ERROR ("Failed to connect to chain. Contacting Master...");
          get_where_to_connect_from_master m_ip m_port () >>= fun _ -> start_client m_ip m_port ()

        end
        | Core.Std.Result.Ok m -> begin

          (* Finish initializing the state with info about the connection *)
          let (a,r,w) = m in
          (*!state.next_node := Some((a,r,w),(ip,port));*) (*Change... see below*)
          let module CIReq = ClientInitRequest in
          let module CIRes = ClientInitResponse in
          let module CReq = ClientRequest(MyWork) in
          let module CRes = ClientResponse(MyWork) in

          debug INFO "Connected to Hydra!";
          debug INFO "Relaying 'worktype' identifier to being initialization.";

          CIReq.send w (CIReq.InitForWorkType(user_id,int_of_string work_id,work_name));
          debug INFO "Waiting on initialization response.";
          CIRes.receive r
          >>= function
          | `Eof  -> begin 
            debug ERROR "Got an error while waiting for initialization response.";
            return()
          end
          | `Ok msg -> begin 
            match msg with
            | CIRes.InitForWorkTypeSucceeded -> begin 
                debug INFO "Successfully initialized for 'worktype' request. Sending our work to Hydra.";
                CReq.send w (CReq.DDWQWorkRequest(MyWork.load_input_for_string work_cmd));
                debug INFO "Work has been sent successfully!";
                return()
               end 
             | CIRes.InitForWorkTypeFailed f -> begin 
                 debug ERROR f;
                 return()
               end
           end
       end
       >>= fun _ -> (after (Core.Std.sec 5.0)) >>= fun _ -> start_client m_ip m_port ()
    end

    else begin 
      debug NONE "Enter the user ID the work request was registered under : ";
      let user_id = Pervasives.input_line stdin in

      debug NONE "Enter corresponding work identification number : ";
      let work_id = Pervasives.input_line stdin in
      
      try_with ( fun () -> (Tcp.connect (Tcp.to_host_and_port (get_node_ip !tail_node) (get_node_port !tail_node))))
      >>= function
      | Core.Std.Result.Error e -> begin 
          debug ERROR ("Failed to connect to chain. Contacting Master...");
          get_where_to_connect_from_master m_ip m_port () >>= fun _ -> start_client m_ip m_port ()
      end
      | Core.Std.Result.Ok m -> begin

        debug INFO "Connected to Hydra!";
        let (a,r,w) = m in
      
        let module CIReq = ClientInitRequest in
        let module CIRes = ClientInitResponse in
        let module CReq = ClientRequest(MyWork) in
        let module CRes = ClientResponse(MyWork) in

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
                             debug WARN "No completed work was found for the specified user work identification.\nYou may want to try again in a minute...";
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
       >>= fun _ -> (after (Core.Std.sec 5.0)) >>= fun _ -> start_client m_ip m_port ()
    end
  end
  else begin  
   return() 
  end


let () =
  Command.async_basic
    ~summary:"Run the DDWQ Client Interface"
    ~readme:WorktypeList.list_worktypes
    Command.Spec.(
      empty
      +> flag "--master-ip"(optional_with_default "localhost" string)
        ~doc:"IP/Hostname of the Hydra node running the Master-Service."
      +> flag "--master-port"(optional_with_default 33334 int)
        ~doc:"Master listening port for Hydra clients."

    )
    (fun ip port () ->

       (Sys.command "clear")
       >>= fun _ ->
          print_main_screen();
          debug NONE "Loading...";
          get_where_to_connect_from_master ip port ()
          >>= fun _ ->
            debug NONE "Loading Done!";
            debug NONE "";
            start_client ip port ()

            >>| fun () -> (shutdown 0)
    )
  |> Command.run