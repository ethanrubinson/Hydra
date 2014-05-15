open Async.Std
open Debug
open Work_full
open Work_addi
open Work_exec
open Protocol
open Async_unix
open AQueue
open Ddwq

(***********************************************************)
(* STATE VARS                                              *)
(***********************************************************)

type which = NEXT | PREV

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
  resp_port   : int ref;

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
    resp_port   = ref (-1);

    next_node = ref None;
    prev_node = ref None;

    id    = ref ("",-1);
    mconn = ref None;
    self  = ref None;
  }


let history = ref (Hashtbl.create 100)
let last_acked_seq_num_received = ref (-1)
let last_sent_seq_num = ref (-1)

(*let should_terminate = Ivar.create()*)
let should_send_new_tail_ack = Ivar.create()


let should_terminate_current_next_conn = ref (Ivar.create())
let next_conn_terminated = ref (Ivar.create())

let should_terminate_current_prev_conn = ref (Ivar.create())
let prev_conn_terminated = ref (Ivar.create())


let should_terminate_client_responding_service = ref (Ivar.create())


let our_state_mutex = Mutex.create()

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

  (Sys.command "clear")
  >>= fun _ ->
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
     let history_output_string = ref "" in
     output_string := !output_string ^ "SELF=" ^ (self_node_to_string sn) ^ 
                      "\nCONFIRMED=" ^ (string_of_int !last_acked_seq_num_received) ^ 
                      "\nSENT=" ^ (string_of_int !last_sent_seq_num) ^
                      "\nDATA=" ^ 

                      ((if !last_sent_seq_num = -1 then begin
                           history_output_string := "No chain"
                         end
                         else begin
                           let rec print_history key = 
                             let found_entry = Hashtbl.find !history key in
                             (history_output_string := !history_output_string ^ "-> " ^ found_entry);
                             if key < !last_sent_seq_num then begin
                               print_history (key + 1)
                             end
                             else begin
                               ()
                             end
                           in
                           print_history 0
                         end); !history_output_string) ^
                      " -->\n";
   end);

  (if is_none !state.!next_node then begin
      output_string := !output_string ^ "NEXT=NONE -->\n"
    end
   else begin
     let nn = get_some !state.!next_node in
     output_string := !output_string ^ "NEXT=" ^(next_node_to_string nn) ^ " -->\n";
   end);

  debug NONE ("Visible Chain Structure :" ^ !output_string);
  return ()


let close_socket_and_do_func m f = 
  (Socket.shutdown m `Both);
  !state.mconn := None;
  !state.self  := None;
  ((after (Core.Std.sec (Random.float 5.0))) >>= fun _ -> f())

let find_update_for_seqnum seq_num = 
  Hashtbl.find !history seq_num


let rec search_history_for_work_result prefix =              
  if !last_sent_seq_num = -1 then begin
    None
  end
  else begin
    let rec search_history key = 
      let found_entry = Hashtbl.find !history key in
      if prefix = String.sub found_entry 0 (String.length prefix) then begin
        Some(String.sub found_entry (String.length prefix) ((String.length found_entry) - (String.length prefix)))
      end

      else begin 
        if key < !last_sent_seq_num then begin
          search_history (key + 1)
        end
        else begin
          None
        end
      end
    in
    search_history 0
  end



let launch_client_responding_service () = 

  let module CIReq = ClientInitRequest in
  let module CIRes = ClientInitResponse in
  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port !state.!resp_port)
    (fun addr r w  ->

       let addr_string = Socket.Address.to_string addr in
       debug INFO ("[" ^ addr_string ^ "] Connection established with a client. Starting session");
       debug INFO ("[" ^ addr_string ^ "] Waiting for request from client.");
       CIReq.receive r
       >>= function
       | `Eof -> begin 
           debug WARN ("[" ^ addr_string ^ "] Socket was closed by client. Terminating session.");
           return ()
         end
       | `Ok msg -> begin 
           match msg with 
           | CIReq.InitForWorkType(user_id, work_id, t) -> begin
               let user_info = user_id ^ "|" ^ string_of_int work_id ^ "|" in
               if is_none (Ddwq.get_worktype_for_id t) then begin
                 debug ERROR ("[" ^ addr_string ^ "] Got a worktype request that was not installed.");
                 CIRes.send w (CIRes.InitForWorkTypeFailed("Not installed"));
                 return ()
               end

               else begin 
                 debug INFO ("[" ^ addr_string ^ "] Got a valid worktype request. Sending response.");
                 CIRes.send w CIRes.InitForWorkTypeSucceeded;

                 let m = get_some (Ddwq.get_worktype_for_id t) in
                 let module MyWork = (val m) in 
                 let module CReq = (ClientRequest(MyWork)) in
                 let module CRes = (ClientResponse(MyWork)) in

                 let search_res = search_history_for_work_result user_info in

                 if is_none search_res then begin
                   (*We did not find the work in our history.*)
                   debug WARN ("[" ^ addr_string ^ "] Requesting work that was not found in our history.");
                   CRes.send w (CRes.DDWQWorkResult(None));
                   return()
                 end

                 else begin 
                   (*We found the work in our history! Returning its result*)
                   CRes.send w (CRes.DDWQWorkResult(Some (MyWork.net_data_to_work_output (get_some search_res))));
                   return()
                 end
               end
             end (*case initforworktype*)


         end

    )
  >>= fun server ->
  debug INFO "Started Client TCP Response Server";
  (Ivar.read !should_terminate_client_responding_service)
  >>= fun _ ->
  (Tcp.Server.close server)
  >>= fun _ -> 
  return ()

let launch_client_listening_service () = 

  let module CIReq = ClientInitRequest in
  let module CIRes = ClientInitResponse in
  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port !state.!user_port)
    (fun addr r w  ->

       let addr_string = Socket.Address.to_string addr in
       debug INFO ("[" ^ addr_string ^ "] Connection established with a client. Starting session");
       debug INFO ("[" ^ addr_string ^ "] Waiting for DoWork request from client.");
       CIReq.receive r
       >>= function
       | `Eof -> begin 
           debug WARN ("[" ^ addr_string ^ "] Socket was closed by client. Terminating session.");
           return ()
         end
       | `Ok msg -> begin 
           match msg with 
           | CIReq.InitForWorkType(user_id,work_id,t) -> begin
               let user_info = user_id ^ "|" ^ string_of_int work_id ^ "|" in
               if is_none (Ddwq.get_worktype_for_id t) then begin
                 debug ERROR ("[" ^ addr_string ^ "] Got a worktype request that was not installed.");
                 CIRes.send w (CIRes.InitForWorkTypeFailed("Not installed"));
                 return ()
               end

               else begin 
                 debug INFO ("[" ^ addr_string ^ "] Got a valid worktype request. Sending response.");
                 CIRes.send w CIRes.InitForWorkTypeSucceeded;

                 let m = get_some (Ddwq.get_worktype_for_id t) in

                 let module MyWork = (val m) in 
                 let module CReq = (ClientRequest(MyWork)) in
                 let module CRes = (ClientResponse(MyWork)) in
                 let module Launcher = DdwqController.Make(MyWork) in
                 CReq.receive r
                 >>= function
                 | `Eof -> begin 
                     debug ERROR ("[" ^ addr_string ^ "] Lost connection to client. Terminating session");
                     return ()
                   end 
                 | `Ok msg -> begin 
                     match msg with 
                     | CReq.DDWQWorkRequest(work_input) -> begin 
                         Launcher.run work_input
                         >>=
                         fun work_result -> 
                         let work_result = user_info ^ work_result in
                         let module ChainReq = ChainComm_ReplicaNodeRequest in
                         Mutex.lock our_state_mutex;
                         last_sent_seq_num := !last_sent_seq_num + 1;
                         Hashtbl.add !history (!last_sent_seq_num) work_result;
                         (if not !state.!am_tail then begin 
                             let ((a',r',w'),_) = get_some !state.!next_node in
                             ChainReq.send w' (ChainReq.TakeThisUpdate (!last_sent_seq_num,work_result));
                           end
                          else begin
                            last_acked_seq_num_received := !last_sent_seq_num;
                          end);
                         Mutex.unlock our_state_mutex;
                         return ()
                       end
                   end

               end (*case we have work*)


             end (*case initforworktype*)


         end

    )
  >>= fun server ->
  debug INFO "Started Client TCP Listening Server";
  never()

let rec listen_to_the_chain which () = 
  let module ChainReq = ChainComm_ReplicaNodeRequest in
  let module ChainRes = ChainComm_ReplicaNodeResponse in

  (*Monitor the next node in the chain*)
  if which = NEXT then begin 
    Mutex.lock our_state_mutex;
    let ((a',r',w'),_) = get_some !state.!next_node in
    Mutex.unlock our_state_mutex;

    debug INFO ("Waiting on message from next node.");
    ChainReq.receive r'
    >>= function
    | `Eof -> begin 
        debug ERROR ("Error receving message from next node. Retrying.");
        return()
      end
    | `Ok msg -> begin 
        match msg with
        | ChainReq.TakeThisACK (seq_num_ack) -> begin 
            Mutex.lock our_state_mutex;
            debug INFO ("Got an ACK for SEQ#= " ^ string_of_int seq_num_ack);

            (*Doesn't matter if this is out of sequence. If we get a confirmation for i, everything < i has also been confirmed*)
            last_acked_seq_num_received := seq_num_ack;

            (if !state.!am_head then begin 
                debug INFO ("We are the head. Just recording the ACK. Nothing else");
              end 

             else begin    
               debug INFO ("We are a middle node. Sending the ACK to our prev node");

               let (a,r,w) = get_some !state.!prev_node in
               ChainReq.send w (ChainReq.TakeThisACK(seq_num_ack));
             end); 

            Mutex.unlock our_state_mutex;
            return()
          end 
        | _ -> begin 
            debug ERROR "Got a message that was not TakeThisACK";
            return()
          end
      end
      >>= fun _ ->
      if (Ivar.is_full (!should_terminate_current_next_conn)) then return() else listen_to_the_chain NEXT ()

  end (*of case monitor NEXT*)

  (*Monitor the previous node in the chain*)
  else begin 
    (*We are either the tail or a mid node either way this is the same*)
    Mutex.lock our_state_mutex;
    let (addr,r,w) = get_some !state.!prev_node in
    Mutex.unlock our_state_mutex;

    debug INFO ("Listening for message from prev node.");
    ChainReq.receive r
    >>= function
    | `Eof -> begin 
        debug ERROR ("Error receving message from prev node. Retrying.");
        return()
      end
    | `Ok msg -> begin 
        match msg with
        | ChainReq.UpdateYourHistory(seq_num, hist) -> begin 
            (*We're the tail. Now we have our history. Let the Master-Service know we are ready to rumble*)
            Mutex.lock our_state_mutex;
            debug INFO ("Updating our history to SEQ#= " ^ string_of_int seq_num);
            last_acked_seq_num_received := seq_num;
            last_sent_seq_num := seq_num;
            history := hist;
            Mutex.unlock our_state_mutex;
            debug INFO "Sync completed. Alerting master we are ready to go";
            Ivar.fill_if_empty should_send_new_tail_ack "Yes";
            return()
          end
        | ChainReq.TakeThisUpdate (seq_num_to_send, update) -> begin 
            Mutex.lock our_state_mutex;
            debug INFO ("Got an update with SEQ#= " ^ string_of_int seq_num_to_send ^ " and UPDATE=" ^ update);
            Hashtbl.add !history (seq_num_to_send) update;

            (*we are not the head but we are the tail*)
            (if !state.!am_tail then begin 
                debug INFO ("We are the tail. Updating LastACKRecved and LastSentACK and sending an ACK response");
                last_acked_seq_num_received := seq_num_to_send;
                last_sent_seq_num := seq_num_to_send;
                ChainReq.send w (ChainReq.TakeThisACK(seq_num_to_send));
              end 
            (*We are neither the head or tail*)
             else begin    
               debug INFO ("We are not the tail. Updating LastSentACK (and send the message onwards)");
               last_sent_seq_num := seq_num_to_send;
               let ((a',r',w'),_) = get_some !state.!next_node in
               ChainReq.send w' (ChainReq.TakeThisUpdate(seq_num_to_send,update));
             end); 

            Mutex.unlock our_state_mutex;
            return()
          end 
        | ChainReq.SyncDone -> begin 
            debug INFO "Sync completed. If we are the tail, alert the master. If not, keep chugging";
            Ivar.fill_if_empty should_send_new_tail_ack "Yes";
            return()
          end
        | _ -> begin 
            debug ERROR "Got a message we should not have";
            return()
          end 
      end
      >>= fun _ ->
      if (Ivar.is_full !should_terminate_current_prev_conn) then return() else listen_to_the_chain PREV ()
  end (*Case  monitor PREV*)


let rec prepare_new_tail_node ip port () = 
  (*Alert any eisting connections know they should terminate*)
  Ivar.fill_if_empty (!should_terminate_current_next_conn) (*"YES"*)();
  (*Wait for the connection to terminate. If this is the first instance. It is bypassed by the fill of NO CONNECTION*)
  Ivar.read !next_conn_terminated >>= fun _ ->
  (*Re-create them to block the next connection from coming in*)
  should_terminate_current_next_conn := Ivar.create();
  next_conn_terminated := Ivar.create();


  debug INFO ("Connecting to our new next (tail) node @" ^ ip ^ ":" ^ string_of_int port);
  try_with ( fun () -> (Tcp.connect (Tcp.to_host_and_port ip port)) )
  >>= function
  | Core.Std.Result.Error e -> begin 
      debug ERROR ("Failed to connect node @" ^ ip ^ ":" ^ string_of_int port ^ ". Retrying in 5 seconds");
      ((after (Core.Std.sec 5.0)) >>= fun _ -> Ivar.fill_if_empty (!next_conn_terminated) "NO CONNECTION"; (prepare_new_tail_node ip port ()))
    end
  | Core.Std.Result.Ok m -> begin

      (* Finish initializing the state with info about the connection *)
      let (a,r,w) = m in
      (*!state.next_node := Some((a,r,w),(ip,port));*) (*Change... see below*)
      let module ChainReq = ChainComm_ReplicaNodeRequest in
      let module ChainRes = ChainComm_ReplicaNodeResponse in

      debug INFO "Connected to our next (tail) node. Sending GetReadyToSync packet";
      ChainReq.send w (ChainReq.GetReadyToSync);


      debug INFO "Waiting for DoSyncForState packet.";
      ChainRes.receive r
      >>= function
      | `Eof -> begin 
          debug ERROR "Failed to receive DoSyncForState. Resetting connection and retrying in 5 seconds.";
          ((after (Core.Std.sec 5.0)) >>= fun _ -> Ivar.fill_if_empty (!next_conn_terminated) "NO CONNECTION"; (prepare_new_tail_node ip port ()))
        end
      | `Ok res -> begin 
          match res with 
          | ChainRes.DoSyncForState(next_node_state,next_node_last_sent) -> begin

              (
                (debug INFO ("Got DoSyncForState. Next node has STATE=" ^ string_of_int next_node_state ^ " | SENT_T+=" ^ string_of_int next_node_last_sent));
                (debug INFO ("Waiting on state_mutex so we can get our last_acked_seq_num to sync our new tail"));
                Mutex.lock our_state_mutex;
                !state.next_node := Some((a,r,w),(ip,port));
                !state.am_tail := false;

                (debug INFO ("Got the lock. We have STATE=" ^ string_of_int !last_acked_seq_num_received ^ " | SENT_T+=" ^ string_of_int !last_sent_seq_num));

                (*Check to see if the next_node_state is less than ours | Can only happen if we are the current tail*)
                (if next_node_state < !last_acked_seq_num_received then begin 

                    debug INFO ("State of next < ours. It must be a new node that needs to be sent our history. Sending history @ STATE=" ^ string_of_int !last_acked_seq_num_received);
                    ChainReq.send w (ChainReq.UpdateYourHistory(!last_acked_seq_num_received, !history));

                    (*At this point the tail should be ready to go. It should send its ACK*)

                    debug INFO "Releasing the lock. We have finished initializing our new tail.";


                  end
                 else begin 
                   debug INFO ("State of next >= ours. Sending SyncDone");

                   (*We need to ensure the seq number we are sending our new next node is the next one they are expecting
                     if it is not (we have received more updates than one more than our next node), loop to send them all *)

                   let rec send_missing_updates next_last_sent =
                     if (!last_sent_seq_num > next_last_sent) then begin
                       ChainReq.send w (ChainReq.TakeThisUpdate (next_node_last_sent + 1, (find_update_for_seqnum (next_last_sent + 1))));
                       send_missing_updates (next_last_sent+1)
                     end
                     else begin
                       ()
                     end                
                   in
                   send_missing_updates next_node_last_sent;

                   ChainReq.send w (ChainReq.SyncDone);


                 end);
                Mutex.unlock our_state_mutex;
              );
              (*Don't close the socket to our next node until we are told to*)        
              listen_to_the_chain NEXT () 
              (*At this point our tail is guarenteed to have THEIR:STATE > OUR:STATE ie. their:last_acked_seq_num > our:last_acked_seq_num*)

            end

        end



        >>= fun _ ->
        (Ivar.fill_if_empty (!next_conn_terminated) "Yes");
        return ()
    end


(*This is only called once. Only one server can be created on the listening port*)
let rec init_as_new_tail () = 
  let module ChainReq = ChainComm_ReplicaNodeRequest in
  let module ChainRes = ChainComm_ReplicaNodeResponse in

  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port !state.!chain_port)
    (fun addr r w  ->
       (*let chain_connection = (addr,r,w) in*)
       let addr_string = Socket.Address.to_string addr in
       debug INFO ("[" ^ addr_string ^ "] Connection established with our new prev node. Waiting for curren prev conn (if it exists) to end.");

       (*Alert any eisting connections know they should terminate*)
       Ivar.fill_if_empty (!should_terminate_current_prev_conn) "YES";
       (*Wait for the connection to terminate. If this is the first instance. It is bypassed by the fill of NO CONNECTION*)
       Ivar.read !prev_conn_terminated >>= fun _ ->
       (*Re-create them to block the next connection from coming in*)
       should_terminate_current_prev_conn := Ivar.create();
       prev_conn_terminated := Ivar.create();
       debug INFO ("[" ^ addr_string ^ "] Session started. Waiting on message.");

       ChainReq.receive r
       >>= function
       | `Eof -> begin 
           debug ERROR ("[" ^ addr_string ^ "] Socket was closed by our new prev node. Terminating session.");
           return ()
         end
       | `Ok msg -> begin 
           match msg with
           | ChainReq.GetReadyToSync -> begin 
               debug INFO ("Got ReadyToSync packet from our new prev node.");
               debug INFO ("Waiting on state_mutex so we can respond with our state and seq#.");
               Mutex.lock our_state_mutex;
               !state.prev_node := Some((addr,r,w));
               !state.am_head := false; (*We shouldn't have to do this but it makes everything look nice*)
               debug INFO ("Got the lock. We have STATE=" ^ string_of_int !last_acked_seq_num_received ^ " | SENT_T+=" ^ string_of_int !last_sent_seq_num);


               ChainRes.send w (ChainRes.DoSyncForState(!last_acked_seq_num_received,!last_sent_seq_num));
               Mutex.unlock our_state_mutex;

               (*Don't close the socket to our next node until we are told to*)   
               listen_to_the_chain PREV ()     
               >>= fun _ ->
               (Ivar.fill_if_empty (!prev_conn_terminated) "Yes");
               return ()
             end (* Match GetReadyToSync *)
           | _ -> begin 
               debug INFO ("Got a weird message ????. Terminating session");
               return()
             end
         end

    )
  >>= fun server ->
  debug INFO ("Opening server for our new prev node to connect to");

  never()




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
           debug INFO "We're getting a tail!";



           Ivar.fill_if_empty (!should_terminate_client_responding_service) "YES";
           Ivar.fill_if_empty (!next_conn_terminated) "NO CONNECTION";
           don't_wait_for(prepare_new_tail_node ip port ());
         end
       | MSMonitor.YouHaveNewPrevNode(_) -> begin 
           debug INFO "We have a new prev node!";
         end
       | MSMonitor.YouHaveNewNextNode(ip,port) -> begin 
           debug INFO "We have a new next node!";
           (*Terminate the current connection*)
           don't_wait_for(prepare_new_tail_node ip port ());
         end
       | MSMonitor.YouAreNewTail -> begin
           debug INFO "We are the new tail!";
           (*TODO -- terminate the current tail connection*)
           !state.am_tail := true;
           !state.next_node := None;

           should_terminate_client_responding_service := (Ivar.create());
           don't_wait_for(launch_client_responding_service());
         end
       | MSMonitor.YouAreNewHead -> begin 
           debug INFO "We are the new head!";
           (*Terminate the current head connection*)

           !state.am_head := true; 
           !state.prev_node := None;

           don't_wait_for(launch_client_listening_service());
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
      MSReq.send w (MSReq.InitRequest(fst !state.!id, snd !state.!id, (!state.!user_port,!state.!resp_port)));


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
                      !state.am_tail := true;
                      !state.next_node := None;
                      !state.prev_node := None;

                      (* Okay we've connected now we can wait for user requests*)
                      don't_wait_for(launch_client_listening_service());
                      don't_wait_for(launch_client_responding_service());

                      begin_master_service_listening_service a r w


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
              Ivar.fill_if_empty (!prev_conn_terminated) "NO CONNECTION";
              don't_wait_for(init_as_new_tail ());
              (Ivar.read should_send_new_tail_ack) 
              >>= fun _ ->
              MSAck.send w MSAck.NewTailAck; (*Send the ACK indicating we have initialized ourselves successfully as the tail*)

              (*The additional InitDone from master was removed. It is unnecesary and just opens the door for concurrency issues*)

              (* Ensure these are reset for sanity measures *)
              !state.am_tail := true;
              !state.next_node := None;

              don't_wait_for(launch_client_responding_service());
              begin_master_service_listening_service a r w


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
      +> flag "--master-ip" (optional_with_default "localhost" string)
        ~doc:"IP/Hostname of the Hydra node running the Master-Service."
      +> flag "--master-port" (optional_with_default 33333 int)
        ~doc:"Master listening port for Hydra Replica nodes."
      +> flag "--chain-port" (required int)
        ~doc:"Replica chain communication port"
      +> flag "--request-port" (optional_with_default 20000 int)
        ~doc:"Client interface listening port"
      +> flag "--response-port" (optional_with_default 30000 int)
        ~doc:"Client interface response port"
    )
    (fun ip_master port_master port_chain port_user port_user_resp () ->

       (Sys.command "clear")
       >>= fun _ ->

       debug NONE "########################";
       debug NONE "####  REPLICA NODE  ####";
       debug NONE "########################";
       debug NONE "";

       debug INFO ("Replica node started with Master-IP= " ^ ip_master ^ " Master-Port= " ^ string_of_int port_master ^ " Chain-Port= " ^ string_of_int port_chain ^ " Client-Listen-Port= " ^ string_of_int port_user ^ " Client-Respond-Port= " ^ string_of_int port_user_resp);

       (* Start initializing the state with cmdline args *)
       !state.master_ip   := ip_master;
       !state.master_port := port_master;
       !state.chain_port  := port_chain;
       !state.user_port   := port_user;
       !state.resp_port   := port_user_resp;
       !state.id          := ((Unix.gethostname()), port_chain);
       debug NONE ("Replica Node ID: " ^ node_id_to_string !state.!id);

       ignore(every ~stop:(never()) (Core.Std.sec 1.0) (
           fun () -> don't_wait_for(print_visible_state())
         ));


       begin_master_connection ip_master port_master ()

       >>| fun () -> (shutdown 0)
    )
  |> Command.run
