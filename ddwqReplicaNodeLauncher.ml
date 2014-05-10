open Async.Std
open Debug
open Work_full
open Protocol
open Async_unix
(***********************************************************)
(* STATE VARS                                              *)
(***********************************************************)

type ip_address     = string
type listening_port = int
type node_id        = ip_address * listening_port

type ('a, 'b) t                 = ('a, 'b) Unix_syscalls.Socket.t
type ('a, 'b) master_connection = ([< `Active | `Bound | `Passive | `Unconnected ] as 'a, [< Socket.Address.t ] as 'b) t *
                                  Async_extra.Import.Reader.t * 
                                  Async_extra.Import.Writer.t


type ('a, 'b) next_node_connection  = ('a, 'b) master_connection * node_id
type prev_node_connection           = Async_extra.Import.Socket.Address.Inet.t * Async_extra.Import.Reader.t * Async_extra.Import.Writer.t

type ('a, 'b) self_node = ('a, 'b) master_connection * node_id


type ('a, 'b) node_state = {
  am_head : bool ref;
  am_tail : bool ref;

  master_ip   : string ref;
  master_port : int ref;
  chain_port  : int ref;
  user_port   : int ref;

  next_node : ('a, 'b) next_node_connection option ref;
  prev_node : prev_node_connection option ref;

  id    : node_id ref;
  mconn : ('a, 'b) master_connection option ref;
  self  : ('a, 'b) self_node option ref;

}

let state = ref {
  am_head = ref false;
  am_tail = ref false;

  master_ip   = ref "";
  master_port = ref (-1);
  chain_port  = ref (-1);
  user_port   = ref (-1);

  next_node = ref None;
  prev_node = ref None;

  id    = ref ("",-1);
  mconn = ref None;
  self  = ref None;
}


(*let should_terminate = Ivar.create()*)
let should_send_new_tail_ack = Ivar.create()

(***********************************************************)
(* UTILITY FUNCTIONS                                       *)
(***********************************************************)
let is_none (thing : 'a option) = match thing with | None -> true | _ -> false

let get_some (thing : 'a option) = match thing with | Some x -> x | _ -> failwith "Tried to get Some of None"

let node_id_to_string (nodeId : node_id) : string = 
  let (ip,port) = nodeId in
  "{Node@" ^ ip ^ ":" ^ (string_of_int port) ^ "}"

let next_node_to_string node : string = 
  let (_,nodeId) = node in
  node_id_to_string nodeId

let self_node_to_string node : string = next_node_to_string node

let prev_node_to_string node : string = 
  let (a,_,_) = node in
  "{Node@" ^ Socket.Address.to_string a ^ "}"

(***********************************************************)
(* REPLICA NODE FUNCTIONS                                  *)
(***********************************************************)

(*let rec when_should_terminate () = 
  (Ivar.read should_terminate) >>| fun _ -> ()*)


let print_visible_state () = 
    let output_string = ref "\n" in

    (if is_none !state.!prev_node then begin
      output_string := !output_string ^ "PREV=NONE -->\n";
    end
    else begin
      let pn = get_some !state.!prev_node in
      output_string := !output_string ^ "PREV=" ^ (prev_node_to_string pn) ^ " -->\n";
    end);

    (if is_none !state.!self then begin
      output_string := !output_string ^ "SELF=NONE -->\n";
    end
    else begin
      let sn = get_some !state.!self in
      output_string := !output_string ^ "SELF=" ^ (self_node_to_string sn) ^ " -->\n";
    end);

    (if is_none !state.!next_node then begin
      output_string := !output_string ^ "NEXT=NONE -->\n"
    end
    else begin
      let nn = get_some !state.!next_node in
      output_string := !output_string ^ "NEXT=" ^(next_node_to_string nn) ^ " -->\n";
    end);

    debug INFO ("Visible Chain Structure :" ^ !output_string)


let close_socket_and_do_func m f = 
  (Socket.shutdown m `Both);
  !state.mconn := None;
  !state.self  := None;
  ((after (Core.Std.sec (Random.float 5.0))) >>= fun _ -> f())



let rec prepare_new_tail_node ip port () = 
  debug INFO ("Connecting to our new tail node... @" ^ ip ^ ":" ^ string_of_int port);
  try_with ( fun () -> (Tcp.connect (Tcp.to_host_and_port ip port)) )
  >>= function
    | Core.Std.Result.Error e -> begin 
      debug ERROR "Failed to connect to our tail. Retry in 5 seconds";
      ((after (Core.Std.sec 5.0)) >>= fun _ -> (prepare_new_tail_node ip port ()))
    end
    | Core.Std.Result.Ok m -> begin
      debug INFO "Connected to our tail node!";

      (* Finish initializing the state with info about the connection *)
      let (a,r,w) = m in
      !state.next_node := Some((a,r,w),(ip,port));

      let module ChainReq = ChainComm_ReplicaNodeRequest in
      let module ChainRes = ChainComm_ReplicaNodeResponse in


      (* Get our initialization type from the master. This is either FirstChainMember or NewTail*)
      debug INFO "Sending initialization packet";
      ChainRes.send w (ChainRes.HaveAnUpdate);
      never()
      end

let rec init_as_new_tail () = 
  let module ChainReq = ChainComm_ReplicaNodeRequest in
  let module ChainRes = ChainComm_ReplicaNodeResponse in

  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port !state.!chain_port)
    (fun addr r w  ->
      (*let chain_connection = (addr,r,w) in*)
      let addr_string = Socket.Address.to_string addr in
      debug INFO ("[" ^ addr_string ^ "] Connection established with our prev node. Starting session");
      
      !state.prev_node := Some((addr,r,w));

      ChainRes.receive r
      >>= function
        | `Eof -> begin 
          debug ERROR ("[" ^ addr_string ^ "] Socket was closed by our prev node. Terminating session.");
          return ()
        end
        | `Ok msg -> begin 
            match msg with
              | ChainRes.HaveAnUpdate -> begin 
                debug INFO ("Initialization with prev node successful!");
                (Ivar.fill_if_empty should_send_new_tail_ack "Yes");

                never()
              end (* Match InitReq *)
              | _ -> debug INFO ("Got a weird message ????"); never()

        end

    )
    >>= fun server ->
    debug INFO ("Opening server for our new prev node to connect to");
    (**when_should_terminate()*)
    never()
    (*>>= fun _ ->
    (Tcp.Server.close server)
    >>= fun _ -> 
    return 0*)
  

 
let rec begin_master_service_listening_service a r w = 

  let module MSMonitor = MasterMonitorComm in
  MSMonitor.receive r
  >>= function
    | `Eof -> begin
      debug FATAL "Lost connnection to Master-Service";
      close_socket_and_do_func a (return)
    end
    | `Ok mointor_result -> begin
      (match mointor_result with 
        | MSMonitor.PrepareNewTail(ip, port) -> begin 
          debug INFO "Motherfucking jones.... we're getting a tail! How damn exciting!";
          don't_wait_for(prepare_new_tail_node ip port ());
        end
        | MSMonitor.YouHaveNewPrevNode(_) -> begin 
          debug INFO "We have a new prev node!";
          (*TODO -- terminate the current connection*)
          (*don't_wait_for(init_as_new_tail ());*)
        end
        | MSMonitor.YouHaveNewNextNode(ip,port) -> begin 
          debug INFO "We have a new next node!";
          (*TODO -- terminate the current connection*)
          don't_wait_for(prepare_new_tail_node ip port ());
        end
        | MSMonitor.YouAreNewTail -> begin
          debug INFO "We are the new tail!";
          (*TODO -- terminate the current tail connection*)
          !state.am_tail := true;
          !state.next_node := None;
        end
        | MSMonitor.YouAreNewHead -> begin 
          debug INFO "We are the new head!";
          (*TODO -- terminate the current head connection*)
          !state.am_head := true; 
          !state.prev_node := None;
        end
        | _ -> begin 
          debug ERROR "Got an unexpected message ???";
        end);
      begin_master_service_listening_service a r w
    end






let rec begin_master_connection master_ip master_port () = 
  debug INFO "Attempting to connect to Master-Service...";
  try_with ( fun () -> (Tcp.connect ~timeout:(Core.Std.sec 0.1) (Tcp.to_host_and_port master_ip master_port)) )
  >>= function
    | Core.Std.Result.Error e -> begin 
      debug ERROR "Failed to connect to Master-Service. Retry in 5 seconds";
      ((after (Core.Std.sec 5.0)) >>= fun _ -> (begin_master_connection master_ip master_port ()))
    end
    | Core.Std.Result.Ok m -> begin
      debug INFO "Connected to Master-Service";

      (* Finish initializing the state with info about the connection *)
      let (a,r,w) = m in
      !state.mconn := Some((a,r,w));
      !state.self  := Some((a,r,w) , !state.!id);

      let module MSReq = MasterServiceRequest in
      let module MSRes = MasterServiceResponse in
      let module MSAck = MasterServiceAck in


      (* Get our initialization type from the master. This is either FirstChainMember or NewTail*)
      debug INFO "Sending our initialization request to the Master-Service with our ID";
      MSReq.send w (MSReq.InitRequest(!state.!id));


      debug INFO "Waiting for Master-Service resopnse to initialization request";
      MSRes.receive r
      >>= function
        | `Eof -> begin
          debug ERROR "Failed to receive initialization request from master. Resetting state and retrying in 5 seconds";
          close_socket_and_do_func a (begin_master_connection master_ip master_port)
        end
        
        | `Ok init_response -> begin
          match init_response with 
            | MSRes.FirstChainMember -> begin
              debug INFO "Initialization response received. We are the first chain member";
              debug INFO "Sending FirstChainMemberAck to Master-Service";
              MSAck.send w MSAck.FirstChainMemberAck;

              (*Once we have sent our response. We need to make sure that our request was not orphaned
                by another node requesting to be initialized and was also told they are the first in
                the chain by the master since our ACK may have been delayed...*)
              
              debug INFO "Waiting for either InitDone or InitFailed from Master-Service";
              MSRes.receive r
              >>= function
                 | `Eof -> begin
                    debug ERROR "Failed to receive InitDone or InitFailed. Resetting state and retrying in 5 seconds";
                    close_socket_and_do_func a (begin_master_connection master_ip master_port)
                end
                | `Ok final_init_res -> begin 
                  match final_init_res with
                  | MSRes.InitDone -> begin
                    debug INFO "Our initialization request was successful. We are the first chain member";
                    
                    (* Ensure these are reset for sanity measures *)
                    !state.am_head := true;
                    !state.next_node := None;
                    !state.prev_node := None;

                    (* Okay we've connected now we can wait for user requests.... not sure how to do that just
                       yet so this comment will take its place for now*)
                    begin_master_service_listening_service a r w
                    (*never()  We shall just literally do nothing but keep the socket alive for now*)

                  end
                  | MSRes.InitFailed -> begin
                    debug WARN "Our initialization request was orphaned. Resetting state and retrying in 5 seconds";
                    close_socket_and_do_func a (begin_master_connection master_ip master_port)
                  end
                  | _ -> begin 
                    debug ERROR "Got unexpected response. Expected InitDone or InitFailed. Resetting state and retrying in 5 seconds";
                    close_socket_and_do_func a (begin_master_connection master_ip master_port)
                  end
                end

            end(*FirstChainMember Case*)
            | MSRes.NewTail -> begin
              debug INFO "Initialization response received. We are going to be a new tail node";
             


              (*Listening service start here *)
              don't_wait_for(init_as_new_tail ());
              (Ivar.read should_send_new_tail_ack) 
              >>= fun _ ->
              MSAck.send w MSAck.NewTailAck; (*Send the ACK indicating we have initialized ourselves successfully as the tail*)
              
              (*The additional InitDone from master was removed. It is unnecesary and just opens the door for concurrency issues*)

              (* Ensure these are reset for sanity measures *)
              !state.am_tail := true;
              !state.next_node := None;

              begin_master_service_listening_service a r w
              (*never()  We shall just literally do nothing but keep the socket alive for now*)

              

            end (*NewTail Case*)
            | MSRes.InitFailed -> begin 
              debug ERROR "Initialization failed, another tail node is pending. Retrying in 5 seconds";
              close_socket_and_do_func a (begin_master_connection master_ip master_port)
            end (*Unexpected reponse (wanted FirstChainMember or NewTail) Case*)
            | _ -> begin 
              debug ERROR "Received unexpected initialization response. Resetting state and retrying in 5 seconds";
              close_socket_and_do_func a (begin_master_connection master_ip master_port)
            end (*Unexpected reponse (wanted FirstChainMember or NewTail) Case*)
        end
    end




let () =
  Command.async_basic
    ~summary: "Run the DDWQ Replica Interface"
    ~readme: (fun () -> "This is a replica node of the DDWQ replication chain, the Master-Service must be running before it is launched.")
    Command.Spec.(
      empty
      +> flag "-masterip" (optional_with_default "localhost" string)
         ~doc:"IP/Hostname of Master-Service"
      +> flag "-masterport" (optional_with_default 33333 int)
         ~doc:"IP/Hostname of Master-Service"
      +> flag "-portchain" (required int)
         ~doc:"Replica chain communication port"
      +> flag "-portuser" (required int)
        ~doc:"Client interface communication port"
    )
    (fun ip_master port_master port_chain port_user () ->

      (Sys.command "clear")
      >>= fun _ ->

      debug NONE "########################";
      debug NONE "####  REPLICA NODE  ####";
      debug NONE "########################";
      debug NONE "";

      debug INFO ("Replica node started with Master-IP= " ^ ip_master ^ " Master-Port= " ^ string_of_int port_master ^ " Chain-Port= " ^ string_of_int port_chain ^ " User-Port= " ^ string_of_int port_user);

      (* Start initializing the state with cmdline args *)
      !state.master_ip   := ip_master;
      !state.master_port := port_master;
      !state.chain_port  := port_chain;
      !state.user_port   := port_user;
      !state.id          := ((Unix.gethostname()), port_chain);
      debug NONE ("Replica Node ID: " ^ node_id_to_string !state.!id);

      ignore(every ~stop:(never()) (Core.Std.sec 3.0) (
                  fun () -> print_visible_state()
                ));


      begin_master_connection ip_master port_master ()

      >>| fun () -> (shutdown 0)
    )
  |> Command.run


